/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resource

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	cachev1alpha1 "github.com/hkpark130/kredis-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// LabelsForKredis returns the labels for selecting the resources
func LabelsForKredis(name string, role string) map[string]string {
	return map[string]string{
		"app":                        role,
		"kredis":                     name,
		"app.kubernetes.io/name":     name,
		"app.kubernetes.io/instance": name,
		"role":                       role,
	}
}

func HandleFailedNodes(ctx context.Context, kredis *cachev1alpha1.Kredis, c client.Client, restConfig *rest.Config, scheme *runtime.Scheme) error {
	logger := log.FromContext(ctx)
	logger.Info("Checking for failed Redis nodes")

	// 1. Find a pod to run redis-cli from. Any pod should do. Let's pick the first master pod.
	masterPodName := fmt.Sprintf("%s-master-0", kredis.Name)
	pod := &corev1.Pod{}
	err := c.Get(ctx, types.NamespacedName{Name: masterPodName, Namespace: kredis.Namespace}, pod)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Master pod not found, skipping failed node check for now.", "pod", masterPodName)
			return nil // Not an error, just wait for pod to be created
		}
		logger.Error(err, "Failed to get master pod", "pod", masterPodName)
		return err
	}

	if pod.Status.Phase != corev1.PodRunning {
		logger.Info("Master pod is not running, skipping failed node check.", "pod", masterPodName, "status", pod.Status.Phase)
		return nil
	}

	// 2. Run `redis-cli CLUSTER nodes`
	cmd := []string{"redis-cli", "-p", "6379", "CLUSTER", "nodes"}
	stdout, stderr, err := ExecInPod(ctx, pod.Name, pod.Namespace, cmd, restConfig, scheme)
	if err != nil {
		logger.Error(err, "Failed to execute 'CLUSTER nodes'", "stderr", stderr)
		return err
	}

	// 3. Get all pods in the namespace to check IPs
	podList := &corev1.PodList{}
	if err := c.List(ctx, podList, client.InNamespace(kredis.Namespace)); err != nil {
		logger.Error(err, "Failed to list pods")
		return err
	}
	existingPodIPs := make(map[string]bool)
	for _, p := range podList.Items {
		if p.Status.PodIP != "" {
			existingPodIPs[p.Status.PodIP] = true
		}
	}

	// 4. Parse output and handle failed nodes
	lines := strings.Split(stdout, "\n")
	for _, line := range lines {
		if strings.Contains(line, "fail") {
			fields := strings.Fields(line)
			if len(fields) < 8 {
				continue
			}
			nodeID := fields[0]
			ipPort := fields[1]
			flags := fields[2]
			ip := strings.Split(ipPort, ":")[0]

			pongRecv := fields[5]
			isLongTimeFail := pongRecv == "0"

			_, ipExists := existingPodIPs[ip]

			if (strings.Contains(flags, "fail")) && (!ipExists || isLongTimeFail) {
				logger.Info("Detected a failed node to be removed", "nodeID", nodeID, "ip", ip, "ipExists", ipExists, "isLongTimeFail", isLongTimeFail)

				// 5. Forget the node
				forgetCmd := []string{"redis-cli", "-p", "6379", "CLUSTER", "FORGET", nodeID}
				forgetStdout, forgetStderr, err := ExecInPod(ctx, pod.Name, pod.Namespace, forgetCmd, restConfig, scheme)
				if err != nil {
					logger.Error(err, "Failed to execute 'CLUSTER FORGET'", "nodeID", nodeID, "stderr", forgetStderr)
					// Continue to next failed node
					continue
				}
				logger.Info("Successfully executed 'CLUSTER FORGET'", "nodeID", nodeID, "stdout", forgetStdout)
			}
		}
	}

	return nil
}

// ExecInPod executes a command in a specified pod and returns its stdout and stderr.
func ExecInPod(ctx context.Context, podName, namespace string, command []string, restConfig *rest.Config, scheme *runtime.Scheme) (string, string, error) {
	logger := log.FromContext(ctx)
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		logger.Error(err, "Failed to create clientset")
		return "", "", err
	}

	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec")

	req.VersionedParams(&corev1.PodExecOptions{
		Command: command,
		Stdin:   false,
		Stdout:  true,
		Stderr:  true,
		TTY:     false,
	}, runtime.NewParameterCodec(scheme))

	exec, err := remotecommand.NewSPDYExecutor(restConfig, "POST", req.URL())
	if err != nil {
		logger.Error(err, "Failed to create executor")
		return "", "", err
	}

	var stdout, stderr bytes.Buffer
	err = exec.Stream(remotecommand.StreamOptions{
		Stdin:  nil,
		Stdout: &stdout,
		Stderr: &stderr,
	})

	if err != nil {
		logger.Error(err, "Failed to execute command", "pod", podName, "namespace", namespace, "command", command, "stdout", stdout.String(), "stderr", stderr.String())
		return stdout.String(), stderr.String(), err
	}

	return stdout.String(), stderr.String(), nil
}
