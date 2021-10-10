import adapter from "./adapter.js";
import PORT_NAME from "./port_name.js";

export default (config) => ({
  id: "dynamodb-data-adapter",
  port: PORT_NAME,
  load: () => ({ ...config }), // load env, also does error check before moving on
  link: (env) => (_) => adapter(env) // link adapter
});
