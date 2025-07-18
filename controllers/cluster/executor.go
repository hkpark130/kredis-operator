package cluster

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// PodExecutor handles command execution in pods
type PodExecutor struct {
	clientset  kubernetes.Interface
	restConfig *rest.Config
}

// NewPodExecutor creates a new pod executor
func NewPodExecutor(clientset kubernetes.Interface, restConfig *rest.Config) *PodExecutor {
	return &PodExecutor{
		clientset:  clientset,
		restConfig: restConfig,
	}
}

// ExecResult represents the result of command execution
type ExecResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// ExecuteCommand executes a command in a pod and returns the result
func (pe *PodExecutor) ExecuteCommand(ctx context.Context, pod corev1.Pod, command []string) (*ExecResult, error) {
	logger := log.FromContext(ctx)
	logger.Info("Executing command in pod",
		"pod", pod.Name,
		"namespace", pod.Namespace,
		"command", strings.Join(command, " "))

	req := pe.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec")

	req.VersionedParams(&corev1.PodExecOptions{
		Container: "redis", // Assuming redis container name
		Command:   command,
		Stdin:     false,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(pe.restConfig, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("failed to create executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.Stream(remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
		Tty:    false,
	})

	result := &ExecResult{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
	}

	if err != nil {
		result.ExitCode = 1
		logger.Error(err, "Command execution failed",
			"stdout", result.Stdout,
			"stderr", result.Stderr)
		return result, fmt.Errorf("command execution failed: %w", err)
	}

	result.ExitCode = 0
	logger.Info("Command executed successfully",
		"stdout", result.Stdout,
		"stderr", result.Stderr)

	return result, nil
}

// ExecuteRedisCommand executes a redis-cli command in a pod
func (pe *PodExecutor) ExecuteRedisCommand(ctx context.Context, pod corev1.Pod, port int32, args ...string) (*ExecResult, error) {
	command := []string{"redis-cli", "-p", fmt.Sprintf("%d", port)}
	command = append(command, args...)

	return pe.ExecuteCommand(ctx, pod, command)
}

// CheckRedisClusterInfo gets cluster info from a Redis node
func (pe *PodExecutor) CheckRedisClusterInfo(ctx context.Context, pod corev1.Pod, port int32) (map[string]string, error) {
	result, err := pe.ExecuteRedisCommand(ctx, pod, port, "cluster", "info")
	if err != nil {
		return nil, err
	}

	info := make(map[string]string)
	lines := strings.Split(result.Stdout, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			info[parts[0]] = parts[1]
		}
	}

	return info, nil
}

// IsRedisReady checks if a Redis instance is ready by sending a PING command.
func (pe *PodExecutor) IsRedisReady(ctx context.Context, pod corev1.Pod, port int32) bool {
	result, err := pe.ExecuteRedisCommand(ctx, pod, port, "PING")
	if err != nil {
		return false
	}
	return strings.TrimSpace(result.Stdout) == "PONG"
}

// IsClusterHealthy checks if the Redis cluster state is "ok".
func (pe *PodExecutor) IsClusterHealthy(ctx context.Context, pod corev1.Pod, port int32) (bool, error) {
	info, err := pe.CheckRedisClusterInfo(ctx, pod, port)
	if err != nil {
		return false, nil
	}
	if state, ok := info["cluster_state"]; ok && strings.TrimSpace(state) == "ok" {
		return true, nil
	}
	return false, nil
}

// GetRedisClusterNodes gets cluster nodes information
func (pe *PodExecutor) GetRedisClusterNodes(ctx context.Context, pod corev1.Pod, port int32) ([]RedisNodeInfo, error) {
	result, err := pe.ExecuteRedisCommand(ctx, pod, port, "cluster", "nodes")
	if err != nil {
		return nil, err
	}

	var nodes []RedisNodeInfo
	lines := strings.Split(result.Stdout, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		node, err := parseRedisNodeInfo(line)
		if err != nil {
			continue // Skip invalid lines
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

// CreateCluster initializes a new Redis cluster with the given nodes.
func (pe *PodExecutor) CreateCluster(ctx context.Context, pod corev1.Pod, port int32, nodeIPs []string, replicas int) (*ExecResult, error) {
	logger := log.FromContext(ctx)

	addrs := make([]string, len(nodeIPs))
	for i, ip := range nodeIPs {
		addrs[i] = fmt.Sprintf("%s:%d", ip, port)
	}

	command := []string{"redis-cli", "--cluster", "create"}
	command = append(command, addrs...)
	command = append(command, "--cluster-replicas", fmt.Sprintf("%d", replicas))
	command = append(command, "--cluster-yes")

	logger.Info("Creating cluster with nodes", "command", strings.Join(command, " "))

	return pe.ExecuteCommand(ctx, pod, command)
}

// MeetNode tells a node to meet another node.
func (pe *PodExecutor) MeetNode(ctx context.Context, pod corev1.Pod, port int32, meetIP string, meetPort int32) (*ExecResult, error) {
	logger := log.FromContext(ctx)
	logger.Info("Telling node to meet another node", "pod", pod.Name, "meetIP", meetIP, "meetPort", meetPort)

	args := []string{"cluster", "meet", meetIP, fmt.Sprintf("%d", meetPort)}
	return pe.ExecuteRedisCommand(ctx, pod, port, args...)
}

// ReplicateNode tells a node to replicate a master node.
func (pe *PodExecutor) ReplicateNode(ctx context.Context, pod corev1.Pod, port int32, masterNodeID string) (*ExecResult, error) {
	logger := log.FromContext(ctx)
	logger.Info("Telling node to replicate master", "pod", pod.Name, "masterNodeID", masterNodeID)

	args := []string{"cluster", "replicate", masterNodeID}
	return pe.ExecuteRedisCommand(ctx, pod, port, args...)
}

// AddNodeToCluster adds a new node to the cluster.
func (pe *PodExecutor) AddNodeToCluster(ctx context.Context, newPod corev1.Pod, existingPod corev1.Pod, port int32, masterID string) error {
	logger := log.FromContext(ctx)
	newPodIP := newPod.Status.PodIP
	if newPodIP == "" {
		return fmt.Errorf("new pod %s has no IP address", newPod.Name)
	}

	// Use redis-cli --cluster add-node to add the new node to the cluster
	existingPodAddr := fmt.Sprintf("%s:%d", existingPod.Status.PodIP, port)
	newPodAddr := fmt.Sprintf("%s:%d", newPodIP, port)
	command := []string{"redis-cli", "--cluster", "add-node", newPodAddr, existingPodAddr}
	if masterID != "" {
		command = append(command, "--cluster-slave", "--cluster-master-id", masterID)
	}

	logger.Info("Adding node to cluster with add-node command", "command", strings.Join(command, " "))
	_, err := pe.ExecuteCommand(ctx, existingPod, command)
	if err != nil {
		return fmt.Errorf("failed to execute add-node command for pod %s: %w", newPod.Name, err)
	}

	return nil
}

// RebalanceCluster rebalances cluster slots among all nodes.
func (pe *PodExecutor) RebalanceCluster(ctx context.Context, pod corev1.Pod, port int32) (*ExecResult, error) {
	logger := log.FromContext(ctx)
	logger.Info("Rebalancing cluster", "pod", pod.Name)

	nodeAddr := fmt.Sprintf("%s:%d", pod.Status.PodIP, port)
	command := []string{
		"redis-cli",
		"--cluster",
		"rebalance",
		nodeAddr,
		"--cluster-use-empty-masters",
	}

	return pe.ExecuteCommand(ctx, pod, command)
}

// RedisNodeInfo represents a Redis cluster node
type RedisNodeInfo struct {
	NodeID   string
	IP       string
	Port     int32
	Flags    []string
	MasterID string
	Role     string
	Status   string
}

// parseRedisNodeInfo parses a line from "cluster nodes" output
func parseRedisNodeInfo(line string) (RedisNodeInfo, error) {
	parts := strings.Fields(line)
	if len(parts) < 8 {
		return RedisNodeInfo{}, fmt.Errorf("invalid cluster nodes line: %s", line)
	}

	node := RedisNodeInfo{
		NodeID: parts[0],
		Flags:  strings.Split(parts[2], ","),
	}

	// Parse IP:Port
	ipPort := strings.Split(parts[1], ":")
	if len(ipPort) >= 2 {
		node.IP = ipPort[0]
		// Port parsing would go here
	}

	// Determine role
	for _, flag := range node.Flags {
		if flag == "master" {
			node.Role = "master"
		} else if flag == "slave" {
			node.Role = "slave"
			if len(parts) > 3 && parts[3] != "-" {
				node.MasterID = parts[3]
			}
		}
	}

	// Determine status
	if len(parts) >= 8 {
		linkState := parts[7]
		if linkState == "connected" {
			node.Status = "connected"
		} else {
			node.Status = linkState
		}
	} else if strings.Contains(parts[2], "fail") {
		node.Status = "failed"
	} else if strings.Contains(parts[2], "handshake") {
		node.Status = "handshake"
	} else {
		node.Status = "ready"
	}

	return node, nil
}
