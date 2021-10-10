import { appOpine, core } from "../dev_deps.js";
import dynamodb from "../mod.js";
import PORT_NAME from "../port_name.js";

const hyperConfig = {
  app: appOpine,
  adapters: [{ port: PORT_NAME, plugins: [dynamodb({ region: "us-east-1" })] }]
};

core(hyperConfig);
