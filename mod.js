import createAdapter from "./adapter.js";
import PORT_NAME from "./port_name.js";
import { ApiFactory, DynamoDB } from "./deps.js";

const env = Deno.env.get;

//additional checks may be necessary for env files
const ddb = new ApiFactory({
  credentials: {
    awsAccessKeyId: env("awsAccessKeyId"),
    awsSecretKey: env("awsSecretKey"),
    region: env("region"),
  },
}).makeNew(DynamoDB);

export default (name) => ({
  id: "dynamodb-data-adapter",
  port: PORT_NAME,
  load: () => ddb,
  link: (env) => (_) => createAdapter(env), // link adapter
});
