{{- define "omop-appender.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "omop-appender.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "omop-appender.selectorLabels" -}}
app.kubernetes.io/name: {{ include "omop-appender.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "omop-appender.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{ include "omop-appender.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "omop-appender.probe" -}}
{{- if .root.Values.probes.httpGetPath -}}
httpGet:
  path: {{ .root.Values.probes.httpGetPath }}
  port: http
{{- else -}}
tcpSocket:
  port: http
{{- end }}
{{ toYaml .cfg }}
{{- end }}
