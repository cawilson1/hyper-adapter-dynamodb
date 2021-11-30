import { R, marshall, unmarshall } from "../deps.js";
const { compose, join, map, addIndex, prop, includes } = R;

export const _throw = (error) => {
  throw error;
};

const imap = addIndex(map);

//this fn needs a few tests
// NOTE: Do these make sense as helpers from a single function? Is
//       Object.keys(item) actually expensive? This feels like each should be
//       its own helper function (see utils.review.js)
export const updateExpBuilder = (item) => {
  // NOTE: k and v could live outside of this function
  const k = (i) => `#key${i}`;
  const v = (i) => `:val${i}`;
  const keys = Object.keys(item);

  // NOTE: Creating a composition and immediately calling it foregoes the
  // benefits of creating a named composition (see utils.review.js)
  const assignmentStr = compose(
    join(", "),
    imap((nothingAtAll, i) => `${k(i)} = ${v(i)}`) // NOTE: use _ instead of `nothingAtAll`
  )(keys);

  const updateExp = `SET ${assignmentStr}`; //uses k and v

  // NOTE: Returning this object literal will create a new object on every
  //       iteration, and spreading here will do a lot of unnecessary key+value
  //       looping. Since you own the `{}`, it's okay to mutate it and use only
  //       1 object (see utils.review.js).
  const expAttNames = keys.reduce((acc, el, i) => ({ ...acc, [k(i)]: el }), {}); // uses k

 // NOTE: should this own the marshalling or the caller?
  const expAttVals = marshall(
    keys.reduce((acc, el, i) => ({ ...acc, [v(i)]: item[el] }), {})
  ); // uses v

  // NOTE: Specific abbreviations like this can make this sort of thing
  //       harder to understand for people without context. I recommend spelling
  //       out the words insofar as it makes sense to do so.
  return { updateExp, expAttNames, expAttVals };
};
