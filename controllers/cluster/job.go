package cluster

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
	"github.com/hkpark130/kredis-operator/controllers/resource"
)

// JobType represents the type of cluster operation job
type JobType string

const (
	JobTypeCreate           JobType = "create"
	JobTypeReshard          JobType = "reshard"
	JobTypeRebalance        JobType = "rebalance"
	JobTypeAddNode          JobType = "add-node"
	JobTypeScaleDownMigrate JobType = "scaledown-migrate" // Migrate slots from master to be removed
	JobTypeScaleDownForget  JobType = "scaledown-forget"  // Remove node from cluster
)

// JobManager handles creation and monitoring of cluster operation jobs
type JobManager struct {
	client.Client
}

// NewJobManager creates a new job manager
func NewJobManager(c client.Client) *JobManager {
	return &JobManager{Client: c}
}

// JobStatus represents the status of a job
type JobStatus string

const (
	JobStatusNotFound  JobStatus = "not-found"
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusSucceeded JobStatus = "succeeded"
	JobStatusFailed    JobStatus = "failed"
)

// JobResult contains the result of a job status check
type JobResult struct {
	Status     JobStatus
	Message    string
	JobName    string
	StartTime  *time.Time
	FinishTime *time.Time
}

// generateJobName creates a unique job name for a given operation
func (jm *JobManager) generateJobName(kredis *cachev1alpha1.Kredis, jobType JobType, targetNodeID string) string {
	// Create a short hash of the target node ID for uniqueness
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(targetNodeID)))[:8]
	return fmt.Sprintf("%s-%s-%s", kredis.Name, jobType, hash)
}

// GetJobStatus checks the status of an existing job
func (jm *JobManager) GetJobStatus(ctx context.Context, kredis *cachev1alpha1.Kredis, jobType JobType) (*JobResult, error) {
	logger := log.FromContext(ctx)

	// List all jobs for this Kredis instance and job type
	jobList := &batchv1.JobList{}
	listOpts := []client.ListOption{
		client.InNamespace(kredis.Namespace),
		client.MatchingLabels{
			resource.LabelKredisName: kredis.Name,
			resource.LabelJobType:    string(jobType),
		},
	}

	if err := jm.List(ctx, jobList, listOpts...); err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}

	if len(jobList.Items) == 0 {
		return &JobResult{Status: JobStatusNotFound}, nil
	}

	// Get the most recent job
	job := jm.getMostRecentJob(jobList.Items)
	result := &JobResult{JobName: job.Name}

	if job.Status.StartTime != nil {
		t := job.Status.StartTime.Time
		result.StartTime = &t
	}

	// Check job status
	if job.Status.Succeeded > 0 {
		result.Status = JobStatusSucceeded
		result.Message = "Job completed successfully"
		if job.Status.CompletionTime != nil {
			t := job.Status.CompletionTime.Time
			result.FinishTime = &t
		}
		logger.Info("Job succeeded", "job", job.Name, "type", jobType)
		return result, nil
	}

	if job.Status.Failed > 0 {
		result.Status = JobStatusFailed
		result.Message = fmt.Sprintf("Job failed after %d attempts", job.Status.Failed)
		logger.Info("Job failed", "job", job.Name, "type", jobType, "failures", job.Status.Failed)
		return result, nil
	}

	if job.Status.Active > 0 {
		result.Status = JobStatusRunning
		result.Message = "Job is running"
		logger.V(1).Info("Job is running", "job", job.Name, "type", jobType)
		return result, nil
	}

	result.Status = JobStatusPending
	result.Message = "Job is pending"
	return result, nil
}

// getMostRecentJob returns the most recently created job from a list
func (jm *JobManager) getMostRecentJob(jobs []batchv1.Job) *batchv1.Job {
	if len(jobs) == 0 {
		return nil
	}

	mostRecent := &jobs[0]
	for i := range jobs {
		if jobs[i].CreationTimestamp.After(mostRecent.CreationTimestamp.Time) {
			mostRecent = &jobs[i]
		}
	}
	return mostRecent
}

// CreateReshardJob creates a Job to reshard slots to an empty master
func (jm *JobManager) CreateReshardJob(ctx context.Context, kredis *cachev1alpha1.Kredis, targetNodeID string, targetAddr string, clusterAddr string, slotsToMove int) error {
	logger := log.FromContext(ctx)

	jobName := jm.generateJobName(kredis, JobTypeReshard, targetNodeID)

	// Check if job already exists
	existingJob := &batchv1.Job{}
	if err := jm.Get(ctx, client.ObjectKey{Namespace: kredis.Namespace, Name: jobName}, existingJob); err == nil {
		logger.Info("Reshard job already exists", "job", jobName)
		return nil
	} else if !errors.IsNotFound(err) {
		return fmt.Errorf("failed to check existing job: %w", err)
	}

	// Build reshard command
	command := []string{
		"redis-cli",
		"--cluster", "reshard", clusterAddr,
		"--cluster-to", targetNodeID,
		"--cluster-slots", fmt.Sprintf("%d", slotsToMove),
		"--cluster-from", "all",
		"--cluster-yes",
		"--cluster-pipeline", "100",
	}

	job := resource.BuildClusterOperationJob(kredis, jobName, string(JobTypeReshard), targetNodeID, command)

	if err := jm.Create(ctx, job); err != nil {
		return fmt.Errorf("failed to create reshard job: %w", err)
	}

	logger.Info("Created reshard job", "job", jobName, "targetNode", targetNodeID, "slots", slotsToMove)
	return nil
}

// CreateRebalanceJob creates a Job to rebalance the cluster
func (jm *JobManager) CreateRebalanceJob(ctx context.Context, kredis *cachev1alpha1.Kredis, clusterAddr string) error {
	logger := log.FromContext(ctx)

	// Use a timestamp-based hash for rebalance jobs
	hash := fmt.Sprintf("%d", time.Now().Unix())[:8]
	jobName := fmt.Sprintf("%s-rebalance-%s", kredis.Name, hash)

	// Check if any rebalance job is already running
	jobResult, err := jm.GetJobStatus(ctx, kredis, JobTypeRebalance)
	if err != nil {
		return fmt.Errorf("failed to check existing rebalance job: %w", err)
	}
	if jobResult.Status == JobStatusRunning || jobResult.Status == JobStatusPending {
		logger.Info("Rebalance job already running", "job", jobResult.JobName)
		return nil
	}

	// Build rebalance command
	command := []string{
		"redis-cli",
		"--cluster", "rebalance", clusterAddr,
		"--cluster-use-empty-masters",
		"--cluster-pipeline", "1000",
	}

	job := resource.BuildClusterOperationJob(kredis, jobName, string(JobTypeRebalance), "", command)

	if err := jm.Create(ctx, job); err != nil {
		return fmt.Errorf("failed to create rebalance job: %w", err)
	}

	logger.Info("Created rebalance job", "job", jobName, "clusterAddr", clusterAddr)
	return nil
}

// CreateClusterJob creates a Job to initialize a new Redis cluster
func (jm *JobManager) CreateClusterJob(ctx context.Context, kredis *cachev1alpha1.Kredis, nodeAddrs []string, replicas int) error {
	logger := log.FromContext(ctx)

	// Use a fixed name for cluster creation job
	jobName := fmt.Sprintf("%s-create", kredis.Name)

	// Check if job already exists
	existingJob := &batchv1.Job{}
	if err := jm.Get(ctx, client.ObjectKey{Namespace: kredis.Namespace, Name: jobName}, existingJob); err == nil {
		logger.Info("Create cluster job already exists", "job", jobName)
		return nil
	} else if !errors.IsNotFound(err) {
		return fmt.Errorf("failed to check existing job: %w", err)
	}

	// Build create cluster command
	// redis-cli --cluster create <addr1> <addr2> ... --cluster-replicas N --cluster-yes
	command := []string{"redis-cli", "--cluster", "create"}
	command = append(command, nodeAddrs...)
	command = append(command, "--cluster-replicas", fmt.Sprintf("%d", replicas), "--cluster-yes")

	job := resource.BuildClusterOperationJob(kredis, jobName, string(JobTypeCreate), "", command)

	if err := jm.Create(ctx, job); err != nil {
		return fmt.Errorf("failed to create cluster job: %w", err)
	}

	logger.Info("Created cluster creation job", "job", jobName, "nodes", len(nodeAddrs), "replicas", replicas)
	return nil
}

// CreateAddNodeJob creates a Job to add a node to the cluster
func (jm *JobManager) CreateAddNodeJob(ctx context.Context, kredis *cachev1alpha1.Kredis, newPodAddr string, existingPodAddr string, masterID string) error {
	logger := log.FromContext(ctx)

	// Use new pod address hash for uniqueness
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(newPodAddr)))[:8]
	jobName := fmt.Sprintf("%s-addnode-%s", kredis.Name, hash)

	// Check if job already exists
	existingJob := &batchv1.Job{}
	if err := jm.Get(ctx, client.ObjectKey{Namespace: kredis.Namespace, Name: jobName}, existingJob); err == nil {
		logger.Info("Add-node job already exists", "job", jobName)
		return nil
	} else if !errors.IsNotFound(err) {
		return fmt.Errorf("failed to check existing job: %w", err)
	}

	// Build add-node command
	command := []string{
		"redis-cli",
		"--cluster", "add-node", newPodAddr, existingPodAddr,
	}
	if masterID != "" {
		command = append(command, "--cluster-slave", "--cluster-master-id", masterID)
	}

	job := resource.BuildClusterOperationJob(kredis, jobName, string(JobTypeAddNode), newPodAddr, command)

	if err := jm.Create(ctx, job); err != nil {
		return fmt.Errorf("failed to create add-node job: %w", err)
	}

	logger.Info("Created add-node job", "job", jobName, "newPod", newPodAddr, "isSlave", masterID != "")
	return nil
}

// CreateScaleDownMigrateJob creates a Job to migrate all slots from a master node to other masters.
// This is used during scale-down to safely evacuate data from a master before removal.
// The --cluster-slots parameter uses the total slots held by the source node.
func (jm *JobManager) CreateScaleDownMigrateJob(ctx context.Context, kredis *cachev1alpha1.Kredis, sourceNodeID string, sourceAddr string, clusterAddr string, slotsToMigrate int) error {
	logger := log.FromContext(ctx)

	jobName := jm.generateJobName(kredis, JobTypeScaleDownMigrate, sourceNodeID)

	// Check if job already exists
	existingJob := &batchv1.Job{}
	if err := jm.Get(ctx, client.ObjectKey{Namespace: kredis.Namespace, Name: jobName}, existingJob); err == nil {
		logger.Info("Scale-down migrate job already exists", "job", jobName)
		return nil
	} else if !errors.IsNotFound(err) {
		return fmt.Errorf("failed to check existing job: %w", err)
	}

	// Build reshard command to migrate ALL slots from the source node to other masters
	// This will distribute the slots evenly across remaining masters
	// We use --cluster-from <sourceNodeID> and --cluster-to <any-remaining-master>
	// But for scale-down, we need to migrate TO multiple targets, so we use a different approach:
	// redis-cli --cluster reshard <cluster-addr> --cluster-from <source> --cluster-to <target> --cluster-slots <slots> --cluster-yes
	// Since we want to evacuate ALL slots from source, we iterate or use rebalance with weights
	// For simplicity, we use reshard with "all" remaining masters by running rebalance with --cluster-weight <source>=0

	// Actually, the best approach for scale-down is to use:
	// redis-cli --cluster rebalance <addr> --cluster-weight <sourceNodeID>=0 --cluster-use-empty-masters
	// This will move all slots away from the source node

	command := []string{
		"redis-cli",
		"--cluster", "rebalance", clusterAddr,
		"--cluster-weight", fmt.Sprintf("%s=0", sourceNodeID),
		"--cluster-use-empty-masters",
		"--cluster-pipeline", "100",
		"--cluster-yes",
	}

	job := resource.BuildClusterOperationJob(kredis, jobName, string(JobTypeScaleDownMigrate), sourceNodeID, command)

	if err := jm.Create(ctx, job); err != nil {
		return fmt.Errorf("failed to create scale-down migrate job: %w", err)
	}

	logger.Info("Created scale-down migrate job", "job", jobName, "sourceNode", sourceNodeID, "slots", slotsToMigrate)
	return nil
}

// CreateScaleDownForgetJob creates a Job to remove a node from the cluster using CLUSTER FORGET.
// This must be executed on every remaining node in the cluster.
// The job runs a script that iterates over all cluster nodes and executes CLUSTER FORGET.
func (jm *JobManager) CreateScaleDownForgetJob(ctx context.Context, kredis *cachev1alpha1.Kredis, nodeIDToForget string, clusterAddr string) error {
	logger := log.FromContext(ctx)

	jobName := jm.generateJobName(kredis, JobTypeScaleDownForget, nodeIDToForget)

	// Check if job already exists
	existingJob := &batchv1.Job{}
	if err := jm.Get(ctx, client.ObjectKey{Namespace: kredis.Namespace, Name: jobName}, existingJob); err == nil {
		logger.Info("Scale-down forget job already exists", "job", jobName)
		return nil
	} else if !errors.IsNotFound(err) {
		return fmt.Errorf("failed to check existing job: %w", err)
	}

	// Build forget command
	// CLUSTER FORGET must be executed from every remaining node in the cluster
	// We'll use redis-cli --cluster call to execute on all nodes
	// redis-cli --cluster call <addr> CLUSTER FORGET <nodeID>
	command := []string{
		"redis-cli",
		"--cluster", "call", clusterAddr,
		"CLUSTER", "FORGET", nodeIDToForget,
	}

	job := resource.BuildClusterOperationJob(kredis, jobName, string(JobTypeScaleDownForget), nodeIDToForget, command)

	if err := jm.Create(ctx, job); err != nil {
		return fmt.Errorf("failed to create scale-down forget job: %w", err)
	}

	logger.Info("Created scale-down forget job", "job", jobName, "nodeToForget", nodeIDToForget)
	return nil
}

// GetScaleDownJobStatus checks the status of scale-down jobs for a specific node
func (jm *JobManager) GetScaleDownJobStatus(ctx context.Context, kredis *cachev1alpha1.Kredis, jobType JobType, nodeID string) (*JobResult, error) {
	jobName := jm.generateJobName(kredis, jobType, nodeID)

	job := &batchv1.Job{}
	if err := jm.Get(ctx, client.ObjectKey{Namespace: kredis.Namespace, Name: jobName}, job); err != nil {
		if errors.IsNotFound(err) {
			return &JobResult{Status: JobStatusNotFound}, nil
		}
		return nil, fmt.Errorf("failed to get job %s: %w", jobName, err)
	}

	result := &JobResult{JobName: job.Name}

	if job.Status.StartTime != nil {
		t := job.Status.StartTime.Time
		result.StartTime = &t
	}

	if job.Status.Succeeded > 0 {
		result.Status = JobStatusSucceeded
		result.Message = "Job completed successfully"
		if job.Status.CompletionTime != nil {
			t := job.Status.CompletionTime.Time
			result.FinishTime = &t
		}
		return result, nil
	}

	if job.Status.Failed > 0 {
		result.Status = JobStatusFailed
		result.Message = fmt.Sprintf("Job failed after %d attempts", job.Status.Failed)
		return result, nil
	}

	if job.Status.Active > 0 {
		result.Status = JobStatusRunning
		result.Message = "Job is running"
		return result, nil
	}

	result.Status = JobStatusPending
	result.Message = "Job is pending"
	return result, nil
}

// CleanupCompletedJobs removes old completed/failed jobs for a Kredis instance
func (jm *JobManager) CleanupCompletedJobs(ctx context.Context, kredis *cachev1alpha1.Kredis) error {
	logger := log.FromContext(ctx)

	jobList := &batchv1.JobList{}
	listOpts := []client.ListOption{
		client.InNamespace(kredis.Namespace),
		client.MatchingLabels{
			resource.LabelKredisName: kredis.Name,
		},
	}

	if err := jm.List(ctx, jobList, listOpts...); err != nil {
		return fmt.Errorf("failed to list jobs for cleanup: %w", err)
	}

	for _, job := range jobList.Items {
		// Skip active jobs
		if job.Status.Active > 0 {
			continue
		}

		// Delete completed/failed jobs older than TTL (handled by TTLSecondsAfterFinished, but just in case)
		if job.Status.Succeeded > 0 || job.Status.Failed > 0 {
			deletePolicy := metav1.DeletePropagationBackground
			if err := jm.Delete(ctx, &job, &client.DeleteOptions{
				PropagationPolicy: &deletePolicy,
			}); err != nil && !errors.IsNotFound(err) {
				logger.Error(err, "Failed to delete completed job", "job", job.Name)
			} else {
				logger.V(1).Info("Cleaned up completed job", "job", job.Name)
			}
		}
	}

	return nil
}

// HasIncompleteJobs checks if there are any incomplete (pending, running, or retrying) jobs for a Kredis instance.
// A job is considered "incomplete" if it hasn't reached a terminal state (Complete or Failed condition).
// This is more accurate than checking Active > 0, which misses jobs that are still scheduling.
func (jm *JobManager) HasIncompleteJobs(ctx context.Context, kredis *cachev1alpha1.Kredis) (bool, error) {
	jobList := &batchv1.JobList{}
	listOpts := []client.ListOption{
		client.InNamespace(kredis.Namespace),
		client.MatchingLabels{
			resource.LabelKredisName: kredis.Name,
		},
	}

	if err := jm.List(ctx, jobList, listOpts...); err != nil {
		return false, fmt.Errorf("failed to list jobs: %w", err)
	}

	for _, job := range jobList.Items {
		if !isJobComplete(&job) {
			return true, nil
		}
	}

	return false, nil
}

// isJobComplete checks if a Job has reached a terminal state (Complete or Failed).
// Returns true only when the job has definitely finished (success or all retries exhausted).
func isJobComplete(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		// JobComplete means succeeded, JobFailed means all retries exhausted
		if condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed {
			return true
		}
	}
	return false
}
