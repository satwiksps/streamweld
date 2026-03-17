{{/* Chart and resource names. */}}
{{- define "streamweld.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamweld.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "streamweld.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamweld.labels" -}}
helm.sh/chart: {{ include "streamweld.chart" . }}
app.kubernetes.io/name: {{ include "streamweld.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "streamweld.proxySelectorLabels" -}}
app.kubernetes.io/name: {{ include "streamweld.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: proxy
{{- end -}}

{{- define "streamweld.operatorSelectorLabels" -}}
app.kubernetes.io/name: {{ include "streamweld.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: operator
{{- end -}}

{{- define "streamweld.redisSelectorLabels" -}}
app.kubernetes.io/name: {{ include "streamweld.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: redis
{{- end -}}

{{- define "streamweld.proxyName" -}}
{{- printf "%s-proxy" (include "streamweld.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamweld.operatorName" -}}
{{- printf "%s-operator" (include "streamweld.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamweld.operatorMetricsName" -}}
{{- printf "%s-metrics" (include "streamweld.operatorName" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamweld.dashboardName" -}}
{{- printf "%s-dashboard" (include "streamweld.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamweld.testName" -}}
{{- printf "%s-test-connection" (include "streamweld.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamweld.redisName" -}}
{{- printf "%s-redis" (include "streamweld.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamweld.relayName" -}}
{{- printf "%s-relay" (include "streamweld.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamweld.webhookName" -}}
{{- printf "%s-webhook" (include "streamweld.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamweld.webhookSecretName" -}}
{{- default (printf "%s-tls" (include "streamweld.webhookName" .)) .Values.webhook.existingSecret | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamweld.operatorServiceAccountName" -}}
{{- if .Values.operator.serviceAccount.create -}}
{{- default (include "streamweld.operatorName" .) .Values.operator.serviceAccount.name -}}
{{- else -}}
{{- required "operator.serviceAccount.name is required when serviceAccount.create=false" .Values.operator.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "streamweld.adminSecretName" -}}
{{- default (printf "%s-admin" (include "streamweld.fullname" .)) .Values.operator.adminExistingSecret | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streamweld.redisSecretName" -}}
{{- default (printf "%s-redis" (include "streamweld.fullname" .)) .Values.journal.redis.existingSecret | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Reuse generated credentials on upgrade; generate only on first install. */}}
{{- define "streamweld.adminToken" -}}
{{- if .Values.operator.adminToken -}}
{{- .Values.operator.adminToken -}}
{{- else -}}
{{- $name := include "streamweld.adminSecretName" . -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace $name -}}
{{- if and $existing (hasKey $existing.data "token") -}}
{{- index $existing.data "token" | b64dec -}}
{{- else -}}
{{- randAlphaNum 48 -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "streamweld.redisPassword" -}}
{{- if .Values.redis.auth.password -}}
{{- .Values.redis.auth.password -}}
{{- else -}}
{{- $name := include "streamweld.redisSecretName" . -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace $name -}}
{{- if and $existing (hasKey $existing.data "redis-password") -}}
{{- index $existing.data "redis-password" | b64dec -}}
{{- else -}}
{{- randAlphaNum 40 -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "streamweld.redisURL" -}}
{{- if .Values.journal.redis.url -}}
{{- .Values.journal.redis.url -}}
{{- else if .Values.redis.enabled -}}
{{- if .Values.redis.auth.enabled -}}
{{- printf "redis://:%s@%s:6379/0" (include "streamweld.redisPassword" .) (include "streamweld.redisName" .) -}}
{{- else -}}
{{- printf "redis://%s:6379/0" (include "streamweld.redisName" .) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Cross-field invariants JSON Schema cannot express. */}}
{{- define "streamweld.validateValues" -}}
{{- $couldScale := or (gt (int .Values.proxy.replicaCount) 1) (and .Values.autoscaling.enabled (gt (int .Values.autoscaling.maxReplicas) 1)) -}}
{{- if and $couldScale (eq .Values.journal.backend "memory") -}}
{{- fail "unsafe values: proxy replicas can exceed one only when journal.backend=redis" -}}
{{- end -}}
{{- if and .Values.autoscaling.enabled (gt (int .Values.autoscaling.minReplicas) (int .Values.autoscaling.maxReplicas)) -}}
{{- fail "autoscaling.minReplicas cannot exceed autoscaling.maxReplicas" -}}
{{- end -}}
{{- if gt (int64 .Values.journal.maxBytesPerStream) (int64 .Values.journal.maxTotalBytes) -}}
{{- fail "journal.maxBytesPerStream cannot exceed journal.maxTotalBytes" -}}
{{- end -}}
{{- if and (eq .Values.journal.backend "redis") (not .Values.redis.enabled) (not .Values.journal.redis.url) (not .Values.journal.redis.existingSecret) -}}
{{- fail "journal.backend=redis requires redis.enabled, journal.redis.url, or journal.redis.existingSecret" -}}
{{- end -}}
{{- if and .Values.redis.enabled (ne .Values.journal.backend "redis") -}}
{{- fail "redis.enabled requires journal.backend=redis" -}}
{{- end -}}
{{- if and .Values.redis.enabled (or .Values.journal.redis.url .Values.journal.redis.existingSecret) -}}
{{- fail "embedded redis.enabled cannot be combined with journal.redis.url or journal.redis.existingSecret" -}}
{{- end -}}
{{- if and .Values.relay.enabled (ne .Values.journal.backend "redis") -}}
{{- fail "relay.enabled requires journal.backend=redis" -}}
{{- end -}}
{{- if and .Values.relay.enabled (not .Values.relay.tls.existingSecret) -}}
{{- fail "relay.enabled requires relay.tls.existingSecret containing the relay CA, certificate, and private key" -}}
{{- end -}}
{{- if and .Values.relay.enabled (eq (int .Values.relay.port) 8080) -}}
{{- fail "relay.port must differ from the proxy HTTP port 8080" -}}
{{- end -}}
{{- if and .Values.operator.adminExistingSecret .Values.operator.adminToken -}}
{{- fail "operator.adminExistingSecret and operator.adminToken are mutually exclusive" -}}
{{- end -}}
{{- if and .Values.webhook.enabled (not .Values.operator.enabled) -}}
{{- fail "webhook.enabled requires operator.enabled=true" -}}
{{- end -}}
{{- if and .Values.operator.enabled (or (eq (int .Values.operator.drain.port) 8080) (eq (int .Values.operator.drain.port) 8081)) -}}
{{- fail "operator.drain.port must differ from the operator metrics and health ports 8080 and 8081" -}}
{{- end -}}
{{- if and .Values.webhook.enabled (or (eq (int .Values.webhook.port) 8080) (eq (int .Values.webhook.port) 8081)) -}}
{{- fail "webhook.port must differ from the operator metrics and health ports 8080 and 8081" -}}
{{- end -}}
{{- if and .Values.webhook.enabled (eq (int .Values.webhook.port) (int .Values.operator.drain.port)) -}}
{{- fail "webhook.port must differ from operator.drain.port" -}}
{{- end -}}
{{- if and .Values.webhook.enabled (not .Values.webhook.existingSecret) (not .Values.webhook.certManager.enabled) -}}
{{- fail "webhook.enabled requires webhook.existingSecret or webhook.certManager.enabled" -}}
{{- end -}}
{{- if and .Values.webhook.enabled (not .Values.webhook.caBundle) (not .Values.webhook.certManager.enabled) -}}
{{- fail "webhook.enabled without cert-manager requires webhook.caBundle" -}}
{{- end -}}
{{- if and (gt (int .Values.operator.replicaCount) 1) (not .Values.operator.leaderElect) -}}
{{- fail "operator replicas greater than one require operator.leaderElect=true" -}}
{{- end -}}
{{- if and .Values.operator.enabled (not .Values.relay.networkPolicy.enabled) -}}
{{- fail "operator.enabled requires relay.networkPolicy.enabled=true to isolate the unauthenticated drain listener" -}}
{{- end -}}
{{- if and .Values.podDisruptionBudget.enabled (ne .Values.podDisruptionBudget.minAvailable nil) (ne .Values.podDisruptionBudget.maxUnavailable nil) -}}
{{- fail "podDisruptionBudget may set only one of minAvailable or maxUnavailable" -}}
{{- end -}}
{{- if and .Values.podDisruptionBudget.enabled (eq .Values.podDisruptionBudget.minAvailable nil) (eq .Values.podDisruptionBudget.maxUnavailable nil) -}}
{{- fail "podDisruptionBudget requires minAvailable or maxUnavailable" -}}
{{- end -}}
{{- end -}}
