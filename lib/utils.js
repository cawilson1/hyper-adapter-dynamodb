import { R, marshall, unmarshall } from "../deps.js";

export const notOk = (error) => ({ ok: false, error });
export const okDoc = (doc) => ({ ok: true, doc: unmarshall(doc.Item) });

export const okDocs = (docs) => ({
  ok: true,
  docs: docs.map(unmarshall),
});
export const okId = (id) => ({ ok: true, id });

const { compose, join, map, addIndex } = R;
const imap = addIndex(map);

//this fn needs a few tests
export const updateExpBuilder = (item) => {
  const k = (i) => `#key${i}`;
  const v = (i) => `:val${i}`;
  const keys = Object.keys(item);
  const assignmentStr = compose(
    join(", "),
    imap((nothingAtAll, i) => `${k(i)} = ${v(i)}`)
  )(keys);

  const updateExp = `SET ${assignmentStr}`; //uses k and v
  const expAttNames = keys.reduce((acc, el, i) => ({ ...acc, [k(i)]: el }), {}); // uses k
  const expAttVals = marshall(
    keys.reduce((acc, el, i) => ({ ...acc, [v(i)]: item[el] }), {})
  ); // uses v
  return { updateExp, expAttNames, expAttVals };
};
