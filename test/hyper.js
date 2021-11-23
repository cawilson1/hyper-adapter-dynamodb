//deno had to be downgraded to v 11.15.3 to prevent some errors importing appOpine
import { appOpine, core } from "../dev_deps.js";
import dynamodb from "../mod.js";
import PORT_NAME from "../port_name.js";


const hyperConfig = {
  app: appOpine,
  adapters: [{ port: PORT_NAME, plugins: [dynamodb()] }]
};

core(hyperConfig);
