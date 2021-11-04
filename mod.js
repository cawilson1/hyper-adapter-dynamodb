import createAdapter from "./adapter.js";
import PORT_NAME from "./port_name.js";
import { ApiFactory, DynamoDB } from "./deps.js";

const ddb = new ApiFactory({ region: "us-east-1" }).makeNew(DynamoDB);

export default (name) => ({
  id: "dynamodb-data-adapter",
  port: PORT_NAME,
  load: () => ddb,
  link: (env) => (_) => createAdapter(env) // link adapter
});
