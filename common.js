
function error (message) {
  console.log(message)
  process.exit()
}

function report (vals) {
  var s = process.stderr
  s.cursorTo(0)
  s.write(Object.keys(vals).map((k) => `${k}: ${vals[k]}`).join('\t'))
  s.clearLine(1)
}

module.exports = {error, report}
