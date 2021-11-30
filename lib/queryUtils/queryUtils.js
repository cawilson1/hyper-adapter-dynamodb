import { marshall, R, unmarshall } from "../../deps.js";
import { _throw } from "../utils.js";
import { TableName } from "../constants.js";
import { queryAll } from "../listDocHelpers.js";

const {
  always,
  compose,
  find,
  flip,
  identity,
  isEmpty,
  prop,
  keys,
  map,
  merge,
  slice,
} = R;

const s = (limit) => (limit ? slice(0, limit) : identity);
export const unmarshallResult = compose(map(unmarshall), s);

//real indexes are not feasible here.
//mimic the functionality of indexes by keeping track of indexes with fields in meta doc
export const retrieveIndexFields = async ({ ddb, db, use_index }) =>
  use_index
    ? await ddb
        .getItem({
          TableName,
          Key: marshall({ pk: db, sk: "meta" }),
        })
        .then(prop("Item"))
        .then(unmarshall)
        .then(prop("indexes"))
        .then(find((el) => el[use_index]))
        .then(prop(use_index))
    : undefined;
