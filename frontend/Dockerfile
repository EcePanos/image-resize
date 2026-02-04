# Frontend Dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json* ./
COPY vite.config.js ./
COPY src ./src
COPY index.html ./
RUN npm install && npm run build
EXPOSE 4173
CMD ["npm", "run", "preview", "--", "--host", "0.0.0.0", "--strictPort"]
