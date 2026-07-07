{{- define "hive-agent.name" -}}hive-agent{{- end -}}
{{- define "hive-agent.fullname" -}}{{ include "hive-agent.name" . }}{{- end -}}
{{- define "hive-agent.labels" -}}
app.kubernetes.io/name: {{ include "hive-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
