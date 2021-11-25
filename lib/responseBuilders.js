import { R, unmarshall } from "../deps.js";
const { compose, map, prop, includes, omit } = R;

const omitPkSk = omit(["pk", "sk"]); //remove partition key and sort key from response

//general
export const ok = () => ({ ok: true });
export const notOk = (error) => ({ ok: false, error });
export const okId = (id) => ({ ok: true, id });
export const okDocs = (docs) => ({
  ok: true,
  docs: compose(map(omitPkSk), map(unmarshall))(docs),
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

const getDocErrorStatus = (error) => {
  if (error === "no doc") return 404;
  return 500;
};
export const notOkGetDoc = (error) => ({
  ok: false,
  status: getDocErrorStatus(error),
  error,
});
export const okGetDoc = (doc) => compose(omitPkSk, unmarshall)(doc.Item);

export const okDeleteDoc = (doc) => {
  const json = compose(omitPkSk, unmarshall)(doc.Attributes);
  return { ok: true, ...json, doc: json };
};

export const okListDocs = (docs) => {
  console.log(docs.Items,"Items",docs.Items.map(unmarshall))
  console.log({
    ok: true,
    docs: compose(map(omitPkSk), map(unmarshall))(docs?.Items),
  });
  return {
    ok: true,
    docs: compose(map(omitPkSk), map(unmarshall))(docs?.Items),
  };
};
export const okQuery = (ddbResponse) => ({
  ok: true,
  docs: compose(map(unmarshall), map(prop("Item")))(ddbResponse.Responses),
});
