exports.characters = {
  columns: { id: 'INT UNSIGNED', name: 'VARCHAR(45)', visits: 'INT UNSIGNED' },
  initial: [
    { id: 1, name: 'Celie', visits: 10 },
    { id: 2, name: 'Nettie', visits: 15 },
    { id: 3, name: 'Harpo', visits: 20 },
    { id: 4, name: 'Shug', visits: 25 },
  ],
  select: 'SELECT * FROM $table$ ORDER BY visits DESC',
  // Optional 'condition' function (not specified on this case)
  // Execute a query after initialization and each update thereafter
  queries: [
    'UPDATE $table$ SET visits=visits+5 WHERE id=4',
    'UPDATE $table$ SET visits=visits+15 WHERE id=1',
    'INSERT INTO $table$ (id, name, visits) VALUES (5, \'Squeak\', 4)',
    'DELETE FROM $table$ WHERE id=3'
  ],
  // Required, an array of diff objects: one for initial data and one for each
  //  query
  expectedDiffs: [
    { removed: null,
      moved: null,
      copied: null,
      added: 
       [ { id: 4, name: 'Shug', visits: 25, _index: 1 },
         { id: 3, name: 'Harpo', visits: 20, _index: 2 },
         { id: 2, name: 'Nettie', visits: 15, _index: 3 },
         { id: 1, name: 'Celie', visits: 10, _index: 4 } ] },
    { removed: [ { _index: 1 } ],
      moved: null,
      copied: null,
      added: [ { id: 4, name: 'Shug', visits: 30, _index: 1 } ] },
    { removed: [ { _index: 2 } ],
      moved: 
       [ { old_index: 2, new_index: 3 },
         { old_index: 3, new_index: 4 } ],
      copied: null,
      added: [ { id: 1, name: 'Celie', visits: 25, _index: 2 } ] },
    { removed: null,
      moved: null,
      copied: null,
      added: [ { id: 5, name: 'Squeak', visits: 4, _index: 5 } ] },
    { removed: [ { _index: 5 } ],
      moved: 
       [ { old_index: 4, new_index: 3 },
         { old_index: 5, new_index: 4 } ],
      copied: null,
      added: null }
  ],
  // Optionally, specify an array of expected data for each event,
  //  in order to test the differ's correctness
  expectedDatas: [
    [ { id: 4, name: 'Shug', visits: 25 },
      { id: 3, name: 'Harpo', visits: 20 },
      { id: 2, name: 'Nettie', visits: 15 },
      { id: 1, name: 'Celie', visits: 10 } ],
    [ { id: 4, name: 'Shug', visits: 30 },
      { id: 3, name: 'Harpo', visits: 20 },
      { id: 2, name: 'Nettie', visits: 15 },
      { id: 1, name: 'Celie', visits: 10 } ],
    [ { id: 4, name: 'Shug', visits: 30 },
      { id: 1, name: 'Celie', visits: 25 },
      { id: 3, name: 'Harpo', visits: 20 },
      { id: 2, name: 'Nettie', visits: 15 } ],
    [ { id: 4, name: 'Shug', visits: 30 },
      { id: 1, name: 'Celie', visits: 25 },
      { id: 3, name: 'Harpo', visits: 20 },
      { id: 2, name: 'Nettie', visits: 15 },
      { id: 5, name: 'Squeak', visits: 4 } ],
    [ { id: 4, name: 'Shug', visits: 30 },
      { id: 1, name: 'Celie', visits: 25 },
      { id: 2, name: 'Nettie', visits: 15 },
      { id: 5, name: 'Squeak', visits: 4 } ]
  ]
}
