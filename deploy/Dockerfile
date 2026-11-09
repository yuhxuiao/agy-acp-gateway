FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tzdata curl procps \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -u 10001 -d /data -M -s /bin/bash agy \
 && mkdir -p /data && chown agy:agy /data

COPY bin/agy-acp-900k bin/agy-acp-gateway /app/bin/
RUN chmod 755 /app/bin/* && ln -s /app/bin/agy-acp-900k /usr/local/bin/agy

# HOME=/data so `agy` login (docker exec) and the gateway's engine share one token + conversation store:
#   /data/.gemini/antigravity-cli/antigravity-oauth-token
ENV HOME=/data \
    TZ=Asia/Shanghai \
    AGY_GW_HOME=/app \
    AGY_ACP_BIN=/app/bin/agy-acp-900k \
    AGY_QUOTA_BIN=/app/bin/agy-acp-900k \
    AGY_PURE_API_HOME=/data/.gemini \
    AGY_DEFAULT_CWD=/data/workspace \
    AGY_STATE_DB=/data/gateway.db \
    AGY_TOS_TRIPPED_FILE=/data/tos-tripped \
    AGY_GW_LOG=/data/logs/gateway.log \
    AGY_LISTEN_HOST=0.0.0.0 \
    AGY_GW_PORT=18890 \
    AGY_DASH_PORT=18891

USER agy
WORKDIR /data
EXPOSE 18890 18891
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s CMD curl -fsS http://127.0.0.1:18890/healthz || exit 1
CMD ["/app/bin/agy-acp-gateway"]
