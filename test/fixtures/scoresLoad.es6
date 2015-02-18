var _ = require('lodash');

var randomString = require('../helpers/randomString');
var querySequence = require('../../src/querySequence');

/**
 * Generate data structure describing a random scores set
 * @param Integer classCount        total number of classes to generate
 * @param Integer assignPerClass    number of assignments per class
 * @param Integer studentsPerClass  number of students enrolled in each class
 * @param Integer classesPerStudent number of classes each student is enrolled
 */
exports.generate =
function(classCount, assignPerClass, studentsPerClass, classesPerStudent) {
  var studentCount = Math.ceil(classCount / classesPerStudent) * studentsPerClass;
  var assignCount = classCount * assignPerClass;
  var scoreCount = assignCount * studentsPerClass;

  var students = _.range(studentCount).map(index => {
    return {
      id: index + 1,
      name: randomString()
    }
  });

  var assignments = _.range(assignCount).map(index => {
    return {
      id: index + 1,
      class_id: (index % classCount) + 1,
      name: randomString(),
      value: Math.ceil(Math.random() * 100)
    }
  });

  var scores = _.range(scoreCount).map(index => {
    var assignId = Math.floor(index / studentsPerClass) + 1;
    var baseStudent = 
      Math.floor((assignments[assignId - 1].class_id - 1) / classesPerStudent);

    return {
      id: index + 1,
      assignment_id: assignId,
      student_id: (baseStudent * studentsPerClass) + (index % studentsPerClass) + 1,
      score: Math.ceil(Math.random() * assignments[assignId - 1].value)
    }
  });

  return { assignments, students, scores };
};

exports.install = function(generation, callback) {
  var insertQuery = (table) => {
    var valueCount = 0;
    return [
      `INSERT INTO ${table}
        (${_.keys(generation[table][0]).join(', ')})
       VALUES
        ${generation[table]
          .map(row => `(
            ${_.map(row, () => '$' + ++valueCount).join(', ')}
          )`).join(', ')}`,
       _.flatten(generation[table].map(row => _.values(row))) ];
  };

  // Create tables, Insert data
  querySequence(client, [
    `DROP TABLE IF EXISTS students CASCADE`,
    `DROP TABLE IF EXISTS assignments CASCADE`,
    `DROP TABLE IF EXISTS scores CASCADE`,
    `CREATE TABLE students (
      id serial NOT NULL,
      name character varying(50) NOT NULL,
      CONSTRAINT students_pkey PRIMARY KEY (id)
    ) WITH ( OIDS=FALSE )`,
    `CREATE TABLE assignments (
      id serial NOT NULL,
      class_id integer NOT NULL,
      name character varying(50),
      value integer NOT NULL,
      CONSTRAINT assignments_pkey PRIMARY KEY (id)
    ) WITH ( OIDS=FALSE )`,
    `CREATE TABLE scores (
      id serial NOT NULL,
      assignment_id integer NOT NULL,
      student_id integer NOT NULL,
      score integer NOT NULL,
      CONSTRAINT scores_pkey PRIMARY KEY (id)
    ) WITH ( OIDS=FALSE )`,
    insertQuery('students'),
    insertQuery('assignments'),
    insertQuery('scores')
  ], callback);
};

