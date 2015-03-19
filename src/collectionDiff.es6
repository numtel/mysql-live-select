
module.exports = function(oldHashes, newData) {
	var curHashes = newData.map(row => row._hash)
	var newHashes = curHashes.filter(hash => oldHashes.indexOf(hash) === -1)

	// Need copy of curHashes so duplicates can be checked off
	var curHashes2 = curHashes.slice()
	var addedRows = newData
		.filter(row => row._added === 1)
		.map(row => {
			// Prepare row meta-data
			row._index = curHashes2.indexOf(row._hash) + 1
			delete row._added

			// Clear this hash so that duplicate hashes can move forward
			curHashes2[row._index - 1] = undefined

			return row
		})

	var movedHashes = curHashes.map((hash, newIndex) => {
		let oldIndex = oldHashes.indexOf(hash)

		if(oldIndex !== -1 &&
				oldIndex !== newIndex &&
				curHashes[oldIndex] !== hash) {
			return {
				old_index: oldIndex + 1,
				new_index: newIndex + 1,
				_hash: hash
			}
		}
	}).filter(moved => moved !== undefined)

	var removedHashes = oldHashes
		.map((_hash, index) => { return { _hash, _index: index + 1 } })
		.filter(removed =>
			curHashes[removed._index - 1] !== removed._hash &&
			movedHashes.filter(moved =>
				moved.new_index === removed._index).length === 0)

	// Add rows that have already existing hash but in new places
	var copiedHashes = curHashes.map((hash, index) => {
		var oldHashIndex = oldHashes.indexOf(hash)
		if(oldHashIndex !== -1 &&
				oldHashes[index] !== hash &&
				movedHashes.filter(moved =>
					moved.new_index - 1 === index).length === 0 &&
				addedRows.filter(added =>
					added._index - 1 === index).length === 0){
			return {
				new_index: index + 1,
				orig_index: oldHashIndex + 1
			}
		}
	}).filter(copied => copied !== undefined)

	var diff = {
		removed: removedHashes.length !== 0 ? removedHashes : null,
		moved: movedHashes.length !== 0 ? movedHashes: null,
		copied: copiedHashes.length !== 0 ? copiedHashes: null,
		added: addedRows.length !== 0 ? addedRows : null
	}

	if(diff.added === null &&
			diff.moved === null &&
			diff.copied === null &&
			diff.removed === null) return null

	return diff
}
