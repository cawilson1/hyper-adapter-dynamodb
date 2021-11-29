import { R, unmarshall } from "../deps.js";
const {
  compose,
  map,
  identity,
  propEq,
  reject,
  includes,
  omit,
  isEmpty,
  tryCatch,
} = R;

const omitPkSk = tryCatch(omit(["pk", "sk"]), () => []); //remove partition key and sort key from response

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

export const okBulkDocs = ({ UnprocessedItems, ids }) =>
  isEmpty(UnprocessedItems)
    ? { ok: true, results: map((id) => ({ ok: true, id }))(ids) }
    : {
        ok: false,
        message:
          "Some of your items were not processed. Please try to upload the UnprocessedItems again.",
        UnprocessedItems,
      };

const filterReservedDocs = reject(propEq("sk", "meta"));
export const okListDocs = (docs) => ({
  ok: true,
  docs: compose(map(omitPkSk), filterReservedDocs, map(unmarshall))(docs),
});

const log = (a) => {
  console.log(a);
  return a;
};
export const okQuery = (ddbResponse) => {
  console.log("THE RESPONSE FROM DDB", ddbResponse);
  return {
    ok: true,
    docs: compose(
      map(omitPkSk),
      filterReservedDocs
    )(ddbResponse),
  };
};
