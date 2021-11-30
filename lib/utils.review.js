//@rpearce - holding off on changes to this. The expression stuff is unused and may not make the final cut.
import { marshall } from "../deps.js";

// NOTE: IMO, all of these need type definitions

export const _throw = (error) => {
  throw error;
};

const k    = (i)    => `#key${i}`;
const v    = (i)    => `:val${i}`;
const kEqV = (_, i) => `${k(i)} = ${v(i)}`;

export const buildExpressionAttributeNames = item =>
  Object.keys(item).reduce((acc, el, i) => {
    acc[k(i)] = el;
    return acc;
  }, {});

export const buildExpressionAttributeValues = item =>
  marshall(
    Object.keys(item).reduce((acc, el, i) => {
      acc[v(i)] = item[el];
      return acc;
    }, {})
  );

export const buildUpdateExpression = item => {
  const assignmentStr = Object.keys(item).map(kEqV).join(", ");
  return `SET ${assignmentStr}`
}

// NOTE: alternative, pointfree `buildUpdateExpression`
//export const buildUpdateExpression =
//  compose(concat("SET "), join(", "), map(kEqV), keys)
