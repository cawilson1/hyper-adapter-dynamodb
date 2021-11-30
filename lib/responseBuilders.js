// NOTE: should this return ({ ok, doc: ... }) like the rest? 
//@rpearce (referring to okGetDoc) no - good eyes though. the docs and hypertest specify a success just return the doc. It matches one of the helpers exactly


// NOTE: On second thought, I would keep these helpers in their functions until
//       that doesn't make sense any more; feels like premature extraction.
//@rpearce (referring to createDocErrorStatus and getDocErrorStatus) - moved inside
import { R, unmarshall } from "../deps.js";
import { EMPTYDOC } from "./constants.js";
const {
  compose,
  map,
  prop,
  propEq,
  reject,
  omit,
  isEmpty,
  tryCatch,
} = R;

const omitPkSk                = tryCatch(omit(["pk", "sk"]), () => []); //remove partition key and sort key from response
const filterReservedDocs      = reject(propEq("sk", "meta")); //each db "table" has a meta doc

const getAttributes           = prop("Attributes");
const getItem                 = prop("Item");

const unmarshallDoc           = compose(omitPkSk, unmarshall);
const unmarshallDocs          = map(unmarshallDoc);
const unmarshallDocAttributes = compose(unmarshallDoc, getAttributes);
const unmarshallDocItem       = compose(unmarshallDoc, getItem);
const removeMetaDoc           = compose(map(omitPkSk), filterReservedDocs)
const unmarshallListDocs      = compose(removeMetaDoc, map(unmarshall))

// =============================================================================
// Ok
// =============================================================================

const ok          = () => ({ ok: true });
const okId        = (id) => ({ ok: true, id });
const log = a => { console.log(a);return a}

export const okCreateDb  = ok
export const okDestroyDb = ok
export const okCreateDoc = okId
export const okGetDoc    = unmarshallDocItem;
export const okDocs      = (docs) => ({ ok: true, docs: unmarshallDocs(docs) });
export const okUpdateDoc = okId
export const okDeleteDoc = (doc) => ({ ok: true, ...unmarshallDocAttributes(doc) });
export const okListDocs  = (docs) => ({ ok: true, docs: unmarshallListDocs(docs) });
export const okQuery     = (ddbResponse) => log({ ok: true, docs: removeMetaDoc(ddbResponse)});
export const okIdxDocs   = ok
export const okBulkDocs  = ({ UnprocessedItems, ids }) =>
  isEmpty(UnprocessedItems)
    ? { ok: true, results: map((id) => ({ ok: true, id }))(ids) }
    : {
        ok: false,
        message:
          "Some of your items were not processed. Please try to upload the UnprocessedItems again.",
        UnprocessedItems,
      };



// =============================================================================
// Errors
// =============================================================================

const notOk          = (error) => ({ ok: false, error });

export const notOkCreateDb  = notOk
export const notOkDestroyDb = notOk
export const notOkUpdateDoc = notOk
export const notOkDeleteDoc = notOk
export const notOkQuery     = notOk
export const notOkListDocs  = notOk
export const notOkBulkDocs  = notOk
export const notOkIdxDocs   = notOk
export const notOkCreateDoc = (error) => {
  const createDocErrorStatus = (error) => {
    if (error === EMPTYDOC) return 400;
    if (error?.message?.includes?.("ConditionalCheckFailedException")) return 409;
    return 500;
  };
  return {
    ok: false,
    status: createDocErrorStatus(error),
    error,
  };
};


export const notOkGetDoc = (error) => {
  const getDocErrorStatus = (error) => {
    if (error === EMPTYDOC) return 404;
    return 500;
  };
  return {
    ok: false,
    status: getDocErrorStatus(error),
    error,
  };
};