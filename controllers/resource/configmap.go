package resource

// ConfigMap functionality removed - not needed for Redis cluster scaling
// The controller handles all cluster management logic, so pods don't need
// to know about TOTAL_MASTERS or other cluster-wide configuration
