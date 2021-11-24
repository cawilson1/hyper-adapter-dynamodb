import { R, unmarshall } from "../deps.js";
const { compose, map, prop, includes, omit } = R;

const omitPkSk = omit(["pk", "sk"]); //remove partition key and sort key from response

//general
export const ok = () => ({ ok: true });
export const notOk = (error) => ({ ok: false, error });
export const okId = (id) => ({ ok: true, id });
export const okDocs = (docs) => ({
  ok: true,

  // NOTE: Two maps composed together can be collapsed to a single composition
  //       for each item thanks to the laws of composition.
  docs: compose(map(omitPkSk), map(unmarshall))(docs),
});

//more specific
const createDocErrorStatus = (error) => {
  if (error === "empty doc") return 400;
  // NOTE: IMO, either own the ramda or nullish aspect of this. See below.
  //if (includes("ConditionalCheckFailedException")(error?.message)) return 409;

  // NOTE: Nullish way (simple enough and probably for the best)
  if (error?.message?.includes?.("ConditionalCheckFailedException")) {
    return 409
  }

  // NOTE: A ramda way.
  //       These 3 would live outside the function and
  //       act as potentially reusable helpers
  //const hasCondCheckFailed   = includes("ConditionalCheckFailedException")
  //const safeMessage          = propOr("", "message")
  //const isCondCheckFailedErr = compose(hasCondCheckFailed, safeMessage)

  //if (isCondCheckFailedErr(error)) {
  //  return 409
  //}
  return 500;
};

// NOTE: Another ramda way to write `createDocErrorStatus`
// const createDocErrorStatus = cond([
//   [equals("empty doc")   , always(400)]
//   [isCondCheckFailedError, always(409)]
//   [T                     , always(500)]
// ])
export const notOkCreateDoc = (error) => {
  return {
    ok: false,
    status: createDocErrorStatus(error),
    error,
  };
};

// NOTE: On second thought, I would keep these helpers in their functions until
//       that doesn't make sense any more; feels like premature extraction.
const getDocErrorStatus = (error) => {
  if (error === "no doc") return 404;
  return 500;
};
export const notOkGetDoc = (error) => ({
  ok: false,
  status: getDocErrorStatus(error),
  error,
});
// NOTE: should this return ({ ok, doc: ... }) like the rest?
export const okGetDoc = (doc) => compose(omitPkSk, unmarshall)(doc.Item);

export const okDeleteDoc = (doc) => {
  // NOTE: unmarshalling and omitPkSk are fairly common operations, and since we
  //       can use this same composition in maps, let's extract this and use it
  //       over and over.
  const json = compose(omitPkSk, unmarshall)(doc.Attributes);

  // NOTE: we're sure we want to spread json *and* set json as doc?
  return { ok: true, ...json, doc: json };
};

export const okQuery = (ddbResponse) => ({
  ok: true,
  // NOTE: should this also be running through omitPkSk?
  docs: compose(map(unmarshall), map(prop("Item")))(ddbResponse.Responses),
});
