import { R, unmarshall } from "../deps.js";
const { compose, map, prop, omit } = R;

// NOTE: IMO, every one of these functions should be typed somehow.

const omitPkSk                = omit(["pk", "sk"]);

const getAttributes           = prop('Attributes');
const getItem                 = prop('Item');
const getResponses            = prop('Responses');

const unmarshallDoc           = compose(omitPkSk, unmarshall);
const unmarshallDocs          = map(unmarshallDoc);
const unmarshallDocAttributes = compose(unmarshallDoc, getAttributes);
const unmarshallDocItem       = compose(unmarshallDoc, getItem);
const unmarshallDocResponses  = compose(unmarshallDocItem, getResponses);

// =============================================================================
// Ok
// =============================================================================

export const ok        = ()     => ({ ok: true });
export const okId      = (id)   => ({ ok: true, id });
export const okGetDoc  = (doc)  => ({ ok: true, doc: unmarshallDoc(doc) });
export const okGetDocs = (docs) => ({ ok: true, docs: unmarshallDocs(docs) });

export const okDeleteDoc = (doc) => {
  const json = unmarshallDocAttributes(doc);

  // NOTE: we're sure we want to spread json *and* set json as doc?
  return { ok: true, ...json, doc: json };
};

export const okQuery = (ddbResponse) => ({
  ok: true,
  docs: unmarshallDocResponses(ddbResponse),
});


// =============================================================================
// Errors
// =============================================================================

export const notOk = (error) => ({ ok: false, error });

export const notOkCreateDoc = (error) => {
  let status = 500

  if (error === "empty doc") {
    status = 400
  } else if (error?.message?.includes?.("ConditionalCheckFailedException")) {
    status = 409
  }

  return { ok: false, status, error };
};

export const notOkGetDoc = (error) => {
  const status = error === "no doc" ? 404 : 500;

  return { ok: false, status, error }
};
