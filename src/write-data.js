const AWS = require('aws-sdk');

const dynamodb = new AWS.DynamoDB.DocumentClient({
  apiVersion: '2012-08-10',
  endpoint: new AWS.Endpoint('http://localhost:8000'),
  region: 'us-west-2',
  maxRetries: 5,
  httpOptions:
  {
    timeout: 5000
  }
});

const tableName = 'SchoolStudents';

/**
 * The entry point into the lambda
 *
 * @param {Object} event
 * @param {string} event.schoolId
 * @param {string} event.schoolName
 * @param {string} event.studentId
 * @param {string} event.studentFirstName
 * @param {string} event.studentLastName
 * @param {string} event.studentGrade
 */

function verifyAllFieldsExist(inputObject, allProperties)
{
  console.log("Verifying all fields exist...");

  //Iterate over all valid property names, to determine if they actually exist in the input object.
  var missingProperties = [];

  for(property of allProperties)
  {
    //If property is missing in input object, add it to list of missing properties.
	if(!inputObject.hasOwnProperty(property))
	{
	  missingProperties.push(property);
	}
  }

  //If any fields are missing, log these and throw an error.
  if(missingProperties.length > 0)
  {
	var errMsg = "Input object is missing the following fields: " + missingProperties;
	console.log(errMsg);
	throw new Error(errMsg);
  }

  console.log("All required properties found in input object.");
}

exports.verifyAllFieldsExist = verifyAllFieldsExist;

function verifyAllFieldsAreOfExpectedTypes(inputObject, uuidFields)
{
  console.log("Verifying all fields are of expected types...");

  //We need to check explicitly that uuidFields found on the inputObject are UUID types, whereas all other fields are of string type.
  var invalidTypeProperties = [];

  for(property in inputObject)
  {
    //Perform check against UUID fields.
	if(uuidFields.includes(property))
	{
	  const uuidRegexExp = /^[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}$/gi;
	  var uuidValid = uuidRegexExp.test(inputObject[property]);

	  //If UUID is not valid, log it.
	  if(!uuidValid)
	  {
	    invalidTypeProperties.push(property + " [UUID]");
	  }
	}
	//Perform check against string fields.
	else
	{
	  if(typeof inputObject[property] !== "string")
	  {
	    invalidTypeProperties.push(property + " [string]");
	  }
	}
  }

  if(invalidTypeProperties.length > 0)
  {
    var errMsg = "Input object has incorrect types for the following fields: " + invalidTypeProperties;
	console.log(errMsg);
	throw new Error(errMsg);
  }

  console.log("All fields on input object have valid types.");
}

exports.verifyAllFieldsAreOfExpectedTypes = verifyAllFieldsAreOfExpectedTypes;

function verifyAllFieldsAreNonEmpty(inputObject)
{
  console.log("Verifying all fields are non-empty...");

  var emptyFields = [];

  for(property in inputObject)
  {
    if(inputObject[property] === undefined || inputObject[property].length <= 0)
	{
	  emptyFields.push(property);
	}
  }

  if(emptyFields.length > 0)
  {
	var errMsg = "Empty fields found in input object on the following fields: " + emptyFields;
	console.log(errMsg);
	throw new Error(errMsg);
  }

  console.log("No empty fields found in input object.");
}

exports.verifyAllFieldsAreNonEmpty = verifyAllFieldsAreNonEmpty;

exports.handler = async (event) => {
  //Validate that all expected attributes are present (assume they are all required)
  //Verify all fields exist.
  const allValidFields = ["schoolId", "schoolName", "studentId", "studentFirstName", "studentLastName", "studentGrade"];

  try
  {
    verifyAllFieldsExist(event, allValidFields);
  }
  catch(err)
  {
    throw err;
  }

  //Verify all fields are of expected types.
  const uuidFields = ["schoolId", "studentId"];

  try
  {
	verifyAllFieldsAreOfExpectedTypes(event, uuidFields);
  }
  catch(err)
  {
    throw err;
  }

  //Verify all fields are non-empty.
  try
  {
    verifyAllFieldsAreNonEmpty(event);
  }
  catch(err)
  {
    throw err;
  }

  // Use the AWS.DynamoDB.DocumentClient to save the 'SchoolStudent' record
  // The 'SchoolStudents' table key is composed of schoolId (partition key) and studentId (range key).
  var params =
  {
    TableName : tableName,
    Item:
	{
      'schoolId': event.schoolId,
      'studentId': event.studentId,
	  'schoolName': event.schoolName,
      'studentFirstName': event.studentFirstName,
      'studentLastName': event.studentLastName,
      'studentGrade': event.studentGrade
    }
  };

  console.log("Saving object to DynamoDB...");

  await dynamodb.put(params).promise()
        .then((data) =>
		{
		  console.log("Data successfully written to DB.");
		})
		.catch((err) =>
		{
		  console.log("Error while writing to DB:");
		  console.log(err);
		});
};