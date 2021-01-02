FROM node as jsbuilder

COPY . /app
WORKDIR /app

RUN npm install

# ---------------------------------------------------------

FROM node:slim

COPY --from=jsbuilder /app /app

WORKDIR /app

ENTRYPOINT [ "node", "index.js" ]
