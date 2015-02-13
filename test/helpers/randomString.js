var chars = 'abcdefghijklmnopqrstuvwxyz';

module.exports = function(length) {
  var out = '';
  while(out.length < (length || 10))
    out += chars[Math.floor(Math.random() * chars.length)];
  return out;
}
