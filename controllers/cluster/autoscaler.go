package cluster

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
)

// AutoscaleDecision represents the autoscaling decision
type AutoscaleDecision struct {
	// ShouldScale indicates if scaling is needed
	ShouldScale bool
	// ScaleType indicates the type of scaling (masters-up, masters-down, replicas-up, replicas-down)
	ScaleType string
	// NewMasters is the new number of masters (only set if scaling masters)
	NewMasters int32
	// NewReplicas is the new number of replicas per master (only set if scaling replicas)
	NewReplicas int32
	// Reason provides the reason for scaling decision
	Reason string
}

// AutoscaleResult contains the metrics and decision
type AutoscaleResult struct {
	MemoryUsagePercent int32
	CPUUsagePercent    int32
	Decision           AutoscaleDecision
}

// Autoscaler handles autoscaling logic for Kredis clusters
type Autoscaler struct {
	client.Client
	metricsClient metricsv.Interface
	podExecutor   *PodExecutor
}

// NewAutoscaler creates a new Autoscaler
func NewAutoscaler(c client.Client, metricsClient metricsv.Interface, podExecutor *PodExecutor) *Autoscaler {
	return &Autoscaler{Client: c, metricsClient: metricsClient, podExecutor: podExecutor}
}

// EvaluateAutoscaling evaluates if autoscaling is needed based on current metrics
func (a *Autoscaler) EvaluateAutoscaling(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod) (*AutoscaleResult, error) {
	logger := log.FromContext(ctx)

	spec := kredis.Spec.Autoscaling
	if !spec.Enabled {
		return &AutoscaleResult{Decision: AutoscaleDecision{ShouldScale: false, Reason: "Autoscaling disabled"}}, nil
	}

	// Check if metrics client is available
	if a.metricsClient == nil {
		// #region agent log
		logger.Info("agent_debug",
			"sessionId", "debug-session",
			"runId", "run1",
			"hypothesisId", "A2",
			"location", "controllers/cluster/autoscaler.go:EvaluateAutoscaling",
			"message", "metricsClient is nil (autoscaling disabled at runtime)",
			"data", map[string]interface{}{
				"clusterState": kredis.Status.ClusterState,
				"lastOp":       kredis.Status.LastClusterOperation,
			},
			"timestamp", time.Now().UnixMilli(),
		)
		// #endregion
		return &AutoscaleResult{Decision: AutoscaleDecision{ShouldScale: false, Reason: "Metrics client not available"}}, nil
	}

	// Check if cluster is in a stable state
	if !a.isClusterStable(kredis) {
		// #region agent log
		logger.Info("agent_debug",
			"sessionId", "debug-session",
			"runId", "run1",
			"hypothesisId", "A3",
			"location", "controllers/cluster/autoscaler.go:EvaluateAutoscaling",
			"message", "cluster not stable - skipping autoscaling",
			"data", map[string]interface{}{
				"clusterState":  kredis.Status.ClusterState,
				"lastOp":        kredis.Status.LastClusterOperation,
				"lastScaleType": kredis.Status.LastScaleType,
			},
			"timestamp", time.Now().UnixMilli(),
		)
		// #endregion
		logger.V(1).Info("Cluster not stable, skipping autoscaling evaluation")
		return &AutoscaleResult{Decision: AutoscaleDecision{ShouldScale: false, Reason: "Cluster not stable"}}, nil
	}

	// Note: Stabilization window check is now done per-direction in evaluateScalingDecision
	// This allows different windows for scale-up vs scale-down

	// Collect metrics from Kubernetes Metrics API
	memoryPercent, cpuPercent, err := a.collectMetrics(ctx, kredis, pods)
	if err != nil {
		logger.Error(err, "Failed to collect metrics for autoscaling")
		// #region agent log
		logger.Info("agent_debug",
			"sessionId", "debug-session",
			"runId", "run1",
			"hypothesisId", "A4",
			"location", "controllers/cluster/autoscaler.go:EvaluateAutoscaling",
			"message", "collectMetrics failed (autoscaling decision forced false)",
			"data", map[string]interface{}{
				"err":          err.Error(),
				"podsTotal":    len(pods),
				"clusterState": kredis.Status.ClusterState,
				"lastOp":       kredis.Status.LastClusterOperation,
			},
			"timestamp", time.Now().UnixMilli(),
		)
		// #endregion
		return &AutoscaleResult{
			MemoryUsagePercent: 0,
			CPUUsagePercent:    0,
			Decision:           AutoscaleDecision{ShouldScale: false, Reason: fmt.Sprintf("Failed to collect metrics: %v", err)},
		}, nil // Don't return error to avoid blocking reconciliation
	}

	result := &AutoscaleResult{
		MemoryUsagePercent: memoryPercent,
		CPUUsagePercent:    cpuPercent,
	}

	// Evaluate scaling decision
	// Priority: Memory-based master scaling > CPU-based replica scaling
	decision := a.evaluateScalingDecision(kredis, memoryPercent, cpuPercent)
	result.Decision = decision

	// #region agent log
	logger.Info("agent_debug",
		"sessionId", "debug-session",
		"runId", "run1",
		"hypothesisId", "A5",
		"location", "controllers/cluster/autoscaler.go:EvaluateAutoscaling",
		"message", "autoscaling evaluated",
		"data", map[string]interface{}{
			"memoryPercent": memoryPercent,
			"cpuPercent":    cpuPercent,
			"shouldScale":   decision.ShouldScale,
			"scaleType":     decision.ScaleType,
			"newMasters":    decision.NewMasters,
			"reason":        decision.Reason,
		},
		"timestamp", time.Now().UnixMilli(),
	)
	// #endregion

	if decision.ShouldScale {
		logger.Info("Autoscaling decision made",
			"scaleType", decision.ScaleType,
			"reason", decision.Reason,
			"memoryPercent", memoryPercent,
			"cpuPercent", cpuPercent)
	}

	return result, nil
}

// isClusterStable checks if the cluster is in a state where autoscaling is safe
func (a *Autoscaler) isClusterStable(kredis *cachev1alpha1.Kredis) bool {
	state := kredis.Status.ClusterState
	lastOp := kredis.Status.LastClusterOperation

	// Only autoscale when cluster is Running or Initialized
	if state != string(cachev1alpha1.ClusterStateRunning) &&
		state != string(cachev1alpha1.ClusterStateInitialized) {
		return false
	}

	// Additional check: Don't autoscale if there's an ongoing operation
	// This prevents race conditions where state briefly becomes Running
	// while operations are still in progress
	if strings.Contains(lastOp, "-in-progress") ||
		strings.Contains(lastOp, "-pending") ||
		strings.Contains(lastOp, "-needed") {
		return false
	}

	// Don't autoscale if scale-down is in progress (explicit check for in-progress states)
	if strings.Contains(lastOp, "scaledown-migrate-in-progress") ||
		strings.Contains(lastOp, "scaledown-forget-in-progress") ||
		strings.Contains(lastOp, "scaledown-in-progress") ||
		strings.Contains(lastOp, "scaledown-migrate-retry") {
		return false
	}

	// Don't autoscale immediately after rebalance/reshard operations
	// Wait for data to stabilize across new masters
	if strings.Contains(lastOp, "rebalance-success") ||
		strings.Contains(lastOp, "reshard-success") ||
		strings.Contains(lastOp, "scale-complete") {
		// Check if enough time has passed since the operation
		if kredis.Status.LastScaleTime != nil {
			timeSinceScale := time.Since(kredis.Status.LastScaleTime.Time)
			// Use a minimum stabilization period after rebalance (at least 2 minutes)
			// to allow data distribution to normalize
			minRebalanceStabilization := 2 * time.Minute
			if timeSinceScale < minRebalanceStabilization {
				return false
			}
		}
	}

	// Don't autoscale if there are pending scale-down nodes
	if len(kredis.Status.PendingScaleDown) > 0 {
		return false
	}

	// Don't autoscale if pods are marked for deletion
	if kredis.Status.ScaleDownReady || len(kredis.Status.PodsToDelete) > 0 {
		return false
	}

	return true
}

// hasRecentScaleUp checks if there was a recent scale-up operation that hasn't had
// enough time for data redistribution. This prevents cascading scale-ups.
func (a *Autoscaler) hasRecentScaleUp(kredis *cachev1alpha1.Kredis) bool {
	// Check LastScaleType - only care about master scale-ups
	if kredis.Status.LastScaleType != "masters-up" {
		return false
	}

	// Check if last scale-up was recent (within 5 minutes)
	if kredis.Status.LastScaleTime == nil {
		return false
	}

	timeSinceScale := time.Since(kredis.Status.LastScaleTime.Time)
	// 5 minute window for data redistribution after rebalancing
	redistributionWindow := 5 * time.Minute

	return timeSinceScale < redistributionWindow
}

// canScaleInDirection checks if enough time has passed to allow scaling in the given direction.
// direction should be "up" or "down".
// This considers:
// 1. Time since last scale operation (if available)
// 2. Time since cluster was created (fallback for initial stabilization)
func (a *Autoscaler) canScaleInDirection(kredis *cachev1alpha1.Kredis, direction string) bool {
	spec := kredis.Spec.Autoscaling
	now := time.Now()

	// Determine which stabilization window to use based on intended direction
	var stabilizationSeconds int32
	if direction == "down" {
		stabilizationSeconds = spec.ScaleDownStabilizationWindowSeconds
		if stabilizationSeconds == 0 {
			stabilizationSeconds = 300 // Default 5 minutes for scale down
		}
	} else {
		stabilizationSeconds = spec.ScaleUpStabilizationWindowSeconds
		if stabilizationSeconds == 0 {
			stabilizationSeconds = 60 // Default 1 minute for scale up
		}
	}

	// If LastScaleTime is set, use it as the primary reference
	if kredis.Status.LastScaleTime != nil {
		lastScaleTime := kredis.Status.LastScaleTime.Time
		return now.Sub(lastScaleTime) >= time.Duration(stabilizationSeconds)*time.Second
	}

	// Fallback: Use cluster creation time if LastScaleTime is not set
	// This prevents scaling immediately after cluster creation
	if kredis.CreationTimestamp.Time.IsZero() {
		return true // Shouldn't happen, but allow if no creation time
	}

	creationTime := kredis.CreationTimestamp.Time
	return now.Sub(creationTime) >= time.Duration(stabilizationSeconds)*time.Second
}

// collectMetrics collects CPU and memory metrics
// Memory: Uses Redis INFO memory (used_memory / maxmemory) for masters only, returns MAX across all masters
// CPU: Uses Kubernetes Metrics API for all pods
func (a *Autoscaler) collectMetrics(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod) (memoryPercent, cpuPercent int32, err error) {
	logger := log.FromContext(ctx)

	// Build pod name to pod map for quick lookup
	podMap := make(map[string]*corev1.Pod)
	for i := range pods {
		podMap[pods[i].Name] = &pods[i]
	}

	// Identify master pods from ClusterNodes status
	masterPods := a.getMasterPods(kredis, podMap)
	logger.V(1).Info("Master pods identified", "count", len(masterPods))

	// Collect memory metrics from Redis INFO memory for masters only
	// Use max(used_memory / maxmemory) across all masters as the trigger
	memoryPercent = a.collectRedisMemoryMetrics(ctx, masterPods, kredis.Spec.BasePort)

	// Collect CPU metrics from Kubernetes Metrics API for all pods
	cpuPercent = a.collectCPUMetrics(ctx, kredis, pods)

	logger.V(1).Info("Collected metrics",
		"memoryPercent (max of masters)", memoryPercent,
		"cpuPercent (avg of all)", cpuPercent,
		"masterCount", len(masterPods))

	return memoryPercent, cpuPercent, nil
}

// getMasterPods extracts master pods from ClusterNodes status
func (a *Autoscaler) getMasterPods(kredis *cachev1alpha1.Kredis, podMap map[string]*corev1.Pod) []*corev1.Pod {
	var masterPods []*corev1.Pod

	for _, node := range kredis.Status.ClusterNodes {
		if node.Role != "master" {
			continue
		}
		if pod, ok := podMap[node.PodName]; ok {
			masterPods = append(masterPods, pod)
		}
	}

	return masterPods
}

// collectRedisMemoryMetrics collects memory usage from Redis INFO memory command
// Returns max(used_memory / maxmemory * 100) across all masters
func (a *Autoscaler) collectRedisMemoryMetrics(ctx context.Context, masterPods []*corev1.Pod, basePort int32) int32 {
	logger := log.FromContext(ctx)

	if a.podExecutor == nil || len(masterPods) == 0 {
		return 0
	}

	var maxPercent int32 = 0

	for _, pod := range masterPods {
		if pod.Status.PodIP == "" {
			continue
		}

		// Execute INFO memory command
		result, err := a.podExecutor.ExecuteRedisCommand(ctx, *pod, basePort, "INFO", "memory")
		if err != nil {
			logger.V(1).Info("Failed to get INFO memory", "pod", pod.Name, "error", err)
			continue
		}

		usedMemory, maxMemory := a.parseRedisMemoryInfo(result.Stdout)

		// #region agent log
		logger.Info("agent_debug",
			"sessionId", "debug-session",
			"runId", "run1",
			"hypothesisId", "H1",
			"location", "controllers/cluster/autoscaler.go:collectRedisMemoryMetrics",
			"message", "Redis memory info parsed",
			"data", map[string]interface{}{
				"pod":        pod.Name,
				"usedMemory": usedMemory,
				"maxMemory":  maxMemory,
			},
			"timestamp", time.Now().UnixMilli(),
		)
		// #endregion

		if maxMemory <= 0 {
			// maxmemory not set, skip this master
			logger.Info("maxmemory not set for pod - autoscaling will not work!", "pod", pod.Name, "usedMemory", usedMemory)
			continue
		}

		percent := int32((usedMemory * 100) / maxMemory)
		logger.V(1).Info("Master memory usage",
			"pod", pod.Name,
			"usedMemory", resource.NewQuantity(usedMemory, resource.BinarySI).String(),
			"maxMemory", resource.NewQuantity(maxMemory, resource.BinarySI).String(),
			"percent", percent)

		if percent > maxPercent {
			maxPercent = percent
		}
	}

	return maxPercent
}

// parseRedisMemoryInfo parses Redis INFO memory output and returns used_memory and maxmemory
func (a *Autoscaler) parseRedisMemoryInfo(infoOutput string) (usedMemory, maxMemory int64) {
	lines := strings.Split(infoOutput, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "used_memory":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				usedMemory = v
			}
		case "maxmemory":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				maxMemory = v
			}
		}
	}

	return usedMemory, maxMemory
}

// collectCPUMetrics collects CPU usage from Kubernetes Metrics API for all pods
func (a *Autoscaler) collectCPUMetrics(ctx context.Context, kredis *cachev1alpha1.Kredis, pods []corev1.Pod) int32 {
	logger := log.FromContext(ctx)

	if a.metricsClient == nil {
		return 0
	}

	// Get pod metrics using direct API call (no watch)
	podMetricsList, err := a.metricsClient.MetricsV1beta1().PodMetricses(kredis.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		logger.V(1).Info("Failed to list pod metrics", "error", err)
		return 0
	}

	// Build a map of pod names for this Kredis instance
	kredisPodNames := make(map[string]bool)
	for _, pod := range pods {
		kredisPodNames[pod.Name] = true
	}

	// Build a map of pod resource requests
	podRequests := make(map[string]corev1.ResourceRequirements)
	for _, pod := range pods {
		for _, container := range pod.Spec.Containers {
			if container.Name == "redis" {
				podRequests[pod.Name] = corev1.ResourceRequirements{
					Requests: container.Resources.Requests,
					Limits:   container.Resources.Limits,
				}
				break
			}
		}
	}

	var totalCPUUsage, totalCPURequest int64
	metricsCount := 0

	for _, podMetrics := range podMetricsList.Items {
		if !kredisPodNames[podMetrics.Name] {
			continue
		}

		for _, container := range podMetrics.Containers {
			if container.Name != "redis" {
				continue
			}

			cpuUsage := container.Usage.Cpu().MilliValue()

			requests, ok := podRequests[podMetrics.Name]
			if !ok {
				continue
			}

			cpuRequest := requests.Requests.Cpu()
			if cpuRequest == nil || cpuRequest.MilliValue() == 0 {
				cpuRequest = requests.Limits.Cpu()
			}

			if cpuRequest != nil && cpuRequest.MilliValue() > 0 {
				totalCPUUsage += cpuUsage
				totalCPURequest += cpuRequest.MilliValue()
			}
			metricsCount++
		}
	}

	if metricsCount == 0 || totalCPURequest == 0 {
		return 0
	}

	cpuPercent := int32((totalCPUUsage * 100) / totalCPURequest)

	logger.V(1).Info("Collected CPU metrics",
		"cpuUsageMillis", totalCPUUsage,
		"cpuRequestMillis", totalCPURequest,
		"cpuPercent", cpuPercent,
		"podCount", metricsCount)

	return cpuPercent
}

// evaluateScalingDecision determines if and how to scale based on metrics
// Each scaling decision checks its own stabilization window before proceeding
func (a *Autoscaler) evaluateScalingDecision(kredis *cachev1alpha1.Kredis, memoryPercent, cpuPercent int32) AutoscaleDecision {
	spec := kredis.Spec.Autoscaling
	currentMasters := kredis.Spec.Masters
	currentReplicas := kredis.Spec.Replicas

	// Set defaults if thresholds are not configured
	memScaleUp := spec.MemoryScaleUpThreshold
	if memScaleUp == 0 {
		memScaleUp = 80 // Default 80%
	}
	cpuScaleUp := spec.CPUScaleUpThreshold
	if cpuScaleUp == 0 {
		cpuScaleUp = 80 // Default 80%
	}

	// Set defaults for max
	maxMasters := spec.MaxMasters
	if maxMasters == 0 {
		maxMasters = 100 // Reasonable upper limit
	}
	maxReplicas := spec.MaxReplicasPerMaster
	if maxReplicas == 0 {
		maxReplicas = 5 // Reasonable upper limit
	}

	// Set defaults for min
	minMasters := spec.MinMasters
	if minMasters == 0 {
		minMasters = 3 // Minimum 3 masters for Redis Cluster
	}
	minReplicas := spec.MinReplicasPerMaster
	// minReplicas can be 0 (no replicas), so no default needed

	// Set defaults for scale-down thresholds
	memScaleDown := spec.MemoryScaleDownThreshold
	if memScaleDown == 0 {
		memScaleDown = 30 // Default 30%
	}
	cpuScaleDown := spec.CPUScaleDownThreshold
	if cpuScaleDown == 0 {
		cpuScaleDown = 30 // Default 30%
	}

	// Priority 1: Memory-based master scale-up (for data capacity)
	if memoryPercent >= memScaleUp && currentMasters < maxMasters {
		// Check if scale-up is allowed (stabilization window)
		if !a.canScaleInDirection(kredis, "up") {
			// Can't scale up yet, but maybe scale down is needed - continue checking
		} else {
			// Additional check: If memory is very high (>90%), allow immediate scale-up
			// Otherwise, ensure previous scale operation had time to distribute data
			if memoryPercent < 90 && a.hasRecentScaleUp(kredis) {
				// Recent scale-up detected, wait for data to redistribute
				// This prevents cascading scale-ups before rebalancing takes effect
			} else {
				newMasters := currentMasters + 1
				return AutoscaleDecision{
					ShouldScale: true,
					ScaleType:   "masters-up",
					NewMasters:  newMasters,
					Reason:      fmt.Sprintf("Memory usage %d%% >= threshold %d%%, scaling masters %d -> %d", memoryPercent, memScaleUp, currentMasters, newMasters),
				}
			}
		}
	}

	// Priority 1.5: Memory-based master scale-down (when memory usage is low)
	if memoryPercent <= memScaleDown && currentMasters > minMasters {
		// Check if scale-down is allowed (stabilization window)
		if !a.canScaleInDirection(kredis, "down") {
			// Can't scale down yet - but continue checking other options
		} else {
			newMasters := currentMasters - 1
			return AutoscaleDecision{
				ShouldScale: true,
				ScaleType:   "masters-down",
				NewMasters:  newMasters,
				Reason:      fmt.Sprintf("Memory usage %d%% <= threshold %d%%, scaling masters %d -> %d", memoryPercent, memScaleDown, currentMasters, newMasters),
			}
		}
	}

	// Priority 2: CPU-based replica scale-up (for read capacity)
	if cpuPercent >= cpuScaleUp && currentReplicas < maxReplicas {
		if !a.canScaleInDirection(kredis, "up") {
			// Can't scale up yet
		} else {
			newReplicas := currentReplicas + 1
			return AutoscaleDecision{
				ShouldScale: true,
				ScaleType:   "replicas-up",
				NewReplicas: newReplicas,
				Reason:      fmt.Sprintf("CPU usage %d%% >= threshold %d%%, scaling replicas %d -> %d", cpuPercent, cpuScaleUp, currentReplicas, newReplicas),
			}
		}
	}

	// Priority 2.5: CPU-based replica scale-down (when CPU usage is low)
	if cpuPercent <= cpuScaleDown && currentReplicas > minReplicas {
		if !a.canScaleInDirection(kredis, "down") {
			// Can't scale down yet
		} else {
			newReplicas := currentReplicas - 1
			return AutoscaleDecision{
				ShouldScale: true,
				ScaleType:   "replicas-down",
				NewReplicas: newReplicas,
				Reason:      fmt.Sprintf("CPU usage %d%% <= threshold %d%%, scaling replicas %d -> %d", cpuPercent, cpuScaleDown, currentReplicas, newReplicas),
			}
		}
	}

	return AutoscaleDecision{
		ShouldScale: false,
		Reason:      fmt.Sprintf("No scaling needed (memory: %d%%, cpu: %d%%)", memoryPercent, cpuPercent),
	}
}

// ApplyAutoscaling applies the autoscaling decision by updating the Kredis spec
// NOTE: Only scale-up is supported. Scale-down requires proper Redis Cluster node removal.
func (a *Autoscaler) ApplyAutoscaling(ctx context.Context, kredis *cachev1alpha1.Kredis, decision AutoscaleDecision) error {
	logger := log.FromContext(ctx)

	if !decision.ShouldScale {
		return nil
	}

	// Create a copy and update
	patch := client.MergeFrom(kredis.DeepCopy())

	switch decision.ScaleType {
	case "masters-up":
		kredis.Spec.Masters = decision.NewMasters
		logger.Info("Applying master scale-up", "newMasters", decision.NewMasters, "reason", decision.Reason)
	case "masters-down":
		kredis.Spec.Masters = decision.NewMasters
		logger.Info("Applying master scale-down", "newMasters", decision.NewMasters, "reason", decision.Reason)
	case "replicas-up":
		kredis.Spec.Replicas = decision.NewReplicas
		logger.Info("Applying replica scale-up", "newReplicas", decision.NewReplicas, "reason", decision.Reason)
	case "replicas-down":
		kredis.Spec.Replicas = decision.NewReplicas
		logger.Info("Applying replica scale-down", "newReplicas", decision.NewReplicas, "reason", decision.Reason)
	default:
		// Unsupported scale type - should not happen
		logger.Info("Unsupported scale type", "scaleType", decision.ScaleType)
		return nil
	}

	// Update spec
	if err := a.Patch(ctx, kredis, patch); err != nil {
		return fmt.Errorf("failed to apply autoscaling: %w", err)
	}

	// Update status with scale time
	statusPatch := client.MergeFrom(kredis.DeepCopy())
	now := metav1.Now()
	kredis.Status.LastScaleTime = &now
	kredis.Status.LastScaleType = decision.ScaleType

	if err := a.Status().Patch(ctx, kredis, statusPatch); err != nil {
		logger.Error(err, "Failed to update scale status, but scaling was applied")
		// Don't return error as the scaling was already applied
	}

	return nil
}
