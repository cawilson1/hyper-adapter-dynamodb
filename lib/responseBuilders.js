import { R, unmarshall } from "../deps.js";
const { compose, map, prop, includes } = R;

//general
export const ok = () => ({ ok: true });
export const notOk = (error) => ({ ok: false, error });
export const okId = (id) => ({ ok: true, id });
export const okDoc = (doc) => ({ ok: true, doc: unmarshall(doc.Item) });
export const okDocs = (docs) => ({
  ok: true,
  docs: docs.map(unmarshall),
});

//more specific
const createDocErrorStatus = (error) => {
  if (error === "empty doc") return 400;
  if (includes("ConditionalCheckFailedException")(error?.message)) return 409;
  return 500;
};
export const notOkCreateDoc = (error) => {
  return {
    ok: false,
    status: createDocErrorStatus(error),
    error,
  };
};

export const okQuery = (ddbResponse) => ({
  ok: true,
  docs: compose(map(unmarshall), map(prop("Item")))(ddbResponse.Responses),
});
