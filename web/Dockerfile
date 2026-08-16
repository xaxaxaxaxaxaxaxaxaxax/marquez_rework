FROM node:22-alpine AS build
WORKDIR /usr/src/app

ARG REACT_APP_ADVANCED_SEARCH

COPY package*.json ./
COPY libs ./libs

RUN npm ci
COPY . .
RUN REACT_APP_ADVANCED_SEARCH=$REACT_APP_ADVANCED_SEARCH npm run build

FROM nginx:1.28-alpine

COPY --from=build /usr/src/app/dist /usr/share/nginx/html
COPY docker/default.conf.template /etc/nginx/templates/default.conf.template

EXPOSE 3000
