FROM node:26-alpine

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY app.js config.js ./

RUN addgroup -S exporter && adduser -S exporter -G exporter
USER exporter

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:3001/ping || exit 1

EXPOSE 3001

CMD ["node", "app.js"]
