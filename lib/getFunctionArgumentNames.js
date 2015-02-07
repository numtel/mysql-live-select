"use strict";

module.exports = function (fun) {
  var text = fun.toString();
  var start = text.indexOf("(");
  var end = text.indexOf(")");
  return text.substr(start + 1, end - start - 1).split(",").map(function (arg) {
    return arg.trim();
  });
};