const chai = require('chai');
const assert = chai.assert;
const uuid = require('uuid/v4');
const localDynamoDbUtils = require('./dynamodb-local/dynamodb-local-util');

// software under test
const readData = require('./../src/read-data');
const writeData = require('./../src/write-data');

describe('the code sample', function () {
  this.timeout(10000);

  // This is the test that you want to pass
  it('saves data to DynamoDB and then it can be read', async function () {
    const schoolId = uuid();
    const studentId = uuid();

    const schoolStudent = {
      schoolId: schoolId,
      schoolName: 'Code Sample Academy',
      studentId: studentId,
      studentFirstName: 'Jane',
      studentLastName: 'Doe',
      studentGrade: '8',
    };

    await writeData.handler(schoolStudent);

    const query = {
      schoolId: schoolId,
      studentId: studentId,
    };
    const queryResult = await readData.handler(query);

    assert.isTrue(Array.isArray(queryResult), 'Expected queryResult to be of type Array');
    assert.equal(queryResult.length, 1, 'Expected to find one result');
    assert.deepEqual(queryResult[0], schoolStudent, 'Expected the query result to match what we saved');
  });

  it('(extra credit) can query for SchoolStudent records by studentLastName', async function () {
    const schoolId = uuid();
    const studentId = uuid();

    const schoolStudent = {
      schoolId: schoolId,
      schoolName: 'NWEA Test School',
      studentId: studentId,
      studentFirstName: 'John',
      studentLastName: 'Robertson',
      studentGrade: '5',
    };

    await writeData.handler(schoolStudent);

    const query = {
      studentLastName: schoolStudent.studentLastName,
    };
    const queryResult = await readData.handler(query);

    assert.isTrue(Array.isArray(queryResult), 'Expected queryResult to be of type Array');
    assert.equal(queryResult.length, 1, 'Expected to find one result');
    assert.deepEqual(queryResult[0], schoolStudent, 'Expected the query result to match what we saved');
  });

  it('returns all pages of data', async function () {
    let createdRecords = 0;
    const schoolId = uuid();
    while (createdRecords++ < 10) {
      await writeData.handler({
        schoolId: schoolId,
        schoolName: 'NWEA Test School ' + createdRecords,
        studentId: uuid(),
        studentFirstName: 'Dan',
        studentLastName: 'Danny the ' + createdRecords,
        studentGrade: '3',
      });
    }

    const query = {
      schoolId: schoolId,
    };
    const queryResult = await readData.handler(query);
    assert.isTrue(Array.isArray(queryResult), 'Expected queryResult to be of type Array');
    assert.equal(queryResult.length, 10, 'Expected to find ten results');
  });

  it('validates that a query object with schoolId and studentId returns valid query parameters', function()
  {
	const testSchoolId = "SomeSchoolId";
	const testStudentId = "SomeStudentId";

	const query = {
      schoolId: testSchoolId,
	  studentId: testStudentId
    };

	var params;

	assert.doesNotThrow(() =>
	{
      params = readData.getQueryParams(query);
	}, Error);
	assert.isOk(params);
	assert.hasAllKeys(params, ["TableName", "Key"]);
	assert.hasAllKeys(params.Key, ["schoolId", "studentId"]);
	assert.equal(params.Key.schoolId, testSchoolId, "Expected returned query param schoolId to match the one passed in.");
	assert.equal(params.Key.studentId, testStudentId, "Expected returned query param studentId to match the one passed in.");
  });

  it('validates that a query object with studentLastName returns valid query parameters', function()
  {
	const testStudentLastName = "SomeLastName";

	const query = {
      studentLastName: testStudentLastName
    };

	var params;

	assert.doesNotThrow(() =>
	{
      params = readData.getQueryParams(query);
	}, Error);
	assert.isOk(params);
	assert.hasAllKeys(params, ["TableName", "Limit", "IndexName", "KeyConditionExpression", "ExpressionAttributeValues"]);
	assert.hasAllKeys(params.ExpressionAttributeValues, [":studentLastName"]);
	assert.equal(params.ExpressionAttributeValues[":studentLastName"], testStudentLastName, "Expected returned query param studentLastName to match the one passed in.");
  });

  it('validates that a query object with schoolId returns valid query parameters', function()
  {
	const testSchoolId = "SomeSchoolId";

	const query = {
      schoolId: testSchoolId
    };

	var params;

	assert.doesNotThrow(() =>
	{
      params = readData.getQueryParams(query);
	}, Error);
	assert.isOk(params);
	assert.hasAllKeys(params, ["TableName", "Limit", "KeyConditionExpression", "ExpressionAttributeValues"]);
	assert.hasAllKeys(params.ExpressionAttributeValues, [":schoolId"]);
	assert.equal(params.ExpressionAttributeValues[":schoolId"], testSchoolId, "Expected returned query param schoolId to match the one passed in.");
  });

  it('throws an Error when attempting to query using an invalid query object', async function () {
    const query = {
      studentFirstName: "BadQueryObject",
    };

	assert.throws(() =>
	{
      readData.getQueryParams(query);
	}, Error, "Query not supported! Your query object should supply: (schoolId, studentId) OR (studentLastName) OR (schoolId)"); //Verify the error message is as expected.
  });

  it('validates that all properties exist on input object', function()
  {
	const schoolId = uuid();
    const studentId = uuid();

    const schoolStudent = {
      schoolId: schoolId,
      schoolName: 'Code Sample Academy',
      studentId: studentId,
      studentFirstName: 'Aaron',
      studentLastName: 'Test',
      studentGrade: '12',
    };

	var allValidProperties = ["schoolId", "schoolName", "studentId", "studentFirstName", "studentLastName", "studentGrade"];

    assert.doesNotThrow(() =>
	{
	  writeData.verifyAllFieldsExist(schoolStudent, allValidProperties);
	}, Error);
  });

  it('throws an Error when the input object has missing fields', function()
  {
	const schoolId = uuid();
    const studentId = uuid();

    //Test case should have missing fields for studentLastName, and studentGrade.
    const schoolStudent = {
      schoolId: schoolId,
      schoolName: 'Code Sample Academy',
      studentId: studentId,
      studentFirstName: 'Aaron'
    };

	var allValidProperties = ["schoolId", "schoolName", "studentId", "studentFirstName", "studentLastName", "studentGrade"];

    assert.throws(() =>
	{
	  writeData.verifyAllFieldsExist(schoolStudent, allValidProperties);
	}, Error, "studentLastName,studentGrade"); //Verify the missing fields are captured in the thrown Error.
  });

  it('validates that all properties on input object are of expected types', function()
  {
	const schoolId = uuid();
    const studentId = uuid();

    const schoolStudent = {
      schoolId: schoolId,
      schoolName: 'Code Sample Academy',
      studentId: studentId,
      studentFirstName: 'Aaron',
      studentLastName: 'Test',
      studentGrade: '12',
    };

	var uuidProperties = ["schoolId", "studentId"];

    assert.doesNotThrow(() =>
	{
	  writeData.verifyAllFieldsAreOfExpectedTypes(schoolStudent, uuidProperties);
	}, Error);
  });

  it('throws an Error when input object has UUID fields that are not actually valid UUIDs', function()
  {
	//Intentionally malform UUID values for this check.
	const schoolId = uuid() + "37";
    const studentId = "Hey, this is a lousy UUID! Someone oughta validate this!";

    const schoolStudent = {
      schoolId: schoolId,
      schoolName: 'Code Sample Academy',
      studentId: studentId,
      studentFirstName: 'Aaron',
      studentLastName: 'Test',
      studentGrade: '12',
    };

	var uuidProperties = ["schoolId", "studentId"];

    assert.throws(() =>
	{
	  writeData.verifyAllFieldsAreOfExpectedTypes(schoolStudent, uuidProperties);
	}, Error, "schoolId [UUID],studentId [UUID]"); //Verify the bad UUID fields are captured in the thrown Error.
  });

  it('throws an Error when input object has string fields that are not actually valid strings', function()
  {
	const schoolId = uuid();
    const studentId = uuid();

    //Intentionally supply non-string values into our string fields for this check.
    const schoolStudent = {
      schoolId: schoolId,
      schoolName: 2,
      studentId: studentId,
      studentFirstName: 55,
      studentLastName: false,
      studentGrade: null,
    };

	var uuidProperties = ["schoolId", "studentId"];

    assert.throws(() =>
	{
	  writeData.verifyAllFieldsAreOfExpectedTypes(schoolStudent, uuidProperties);
	}, Error, "schoolName [string],studentFirstName [string],studentLastName [string],studentGrade [string]"); //Verify the bad string fields are captured in the thrown Error.
  });

  it('validates that all properties on input object are non-empty', function()
  {
	const schoolId = uuid();
    const studentId = uuid();

    const schoolStudent = {
      schoolId: schoolId,
      schoolName: 'Code Sample Academy',
      studentId: studentId,
      studentFirstName: 'Aaron',
      studentLastName: 'Test',
      studentGrade: '12',
    };

    assert.doesNotThrow(() =>
	{
	  writeData.verifyAllFieldsAreNonEmpty(schoolStudent);
	}, Error);
  });

  it('throws an Error when input object has fields with empty or undefined values', function()
  {
	const schoolId = uuid();
    const studentId = uuid();

    //Intentionally supply empty/undefined values for this check.
    const schoolStudent = {
      schoolId: schoolId,
      schoolName: 'Code Sample Academy',
      studentId: studentId,
      studentFirstName: '',
      studentLastName: '',
      studentGrade: undefined,
    };

    assert.throws(() =>
	{
	  writeData.verifyAllFieldsAreNonEmpty(schoolStudent);
	}, Error, "studentFirstName,studentLastName,studentGrade"); //Verify the undefined/empty fields are captured in the thrown Error.
  });

  // This section starts the local DynamoDB database
  before(async function () {
    await localDynamoDbUtils.startLocalDynamoDB();

    // create the 'SchoolStudents' DynamoDB table in the locally running database
    const partitionKey = 'schoolId', rangeKey = 'studentId',
      gsiPartitionKey = 'studentLastName', gsiRangeKey = 'studentFirstName';

    const keySchema = [
      { AttributeName: partitionKey, KeyType: "HASH" },
      { AttributeName: rangeKey, KeyType: "RANGE" },
    ];
    const attributeDefinitions = [
      { AttributeName: partitionKey, AttributeType: "S"},
      { AttributeName: rangeKey, AttributeType: "S" },
      { AttributeName: gsiPartitionKey, AttributeType: "S" },
      { AttributeName: gsiRangeKey, AttributeType: "S" },
    ];
    const gsis = [
      localDynamoDbUtils.buildGlobalSecondaryIndex('studentLastNameGsi', [
        {AttributeName: gsiPartitionKey, KeyType: "HASH"},
        {AttributeName: gsiRangeKey, KeyType: "RANGE"}]),
    ];

    await localDynamoDbUtils.createTable('SchoolStudents', keySchema, attributeDefinitions, gsis);
  });

  after(function () {
    localDynamoDbUtils.stopLocalDynamoDB();
  });
});