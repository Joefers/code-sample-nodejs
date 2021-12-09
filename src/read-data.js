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
const studentLastNameGsiName = 'studentLastNameGsi';

/**
 * The entry point into the lambda
 *
 * @param {Object} event
 * @param {string} event.schoolId
 * @param {string} event.studentId
 * @param {string} [event.studentLastName]
 */

function getQueryParams(inputObject)
{
  //If query object supplies both schoolId and studentId, perform a get on DynamoDB to return one object.
  if(inputObject.hasOwnProperty("schoolId") && inputObject.schoolId !== undefined && inputObject.schoolId !== "" && inputObject.schoolId.length > 0
      && inputObject.hasOwnProperty("studentId") && inputObject.studentId !== undefined && inputObject.studentId !== "" && inputObject.studentId.length > 0)
  {
	var params =
    {
      TableName : tableName,
      Key: {
        "schoolId": inputObject.schoolId,
	    "studentId": inputObject.studentId
      }
    };
  }
  //Otherwise, perform a query on DynamoDB to return pages.
  else
  {
	//Limit pages to 5 records each.
	const pageLimit = 5;

	//If studentLastName exists, perform a query on DynamoDB to return limited pages until we get all records.
	if(inputObject.hasOwnProperty("studentLastName") && inputObject.studentLastName !== undefined && inputObject.studentLastName !== "" && inputObject.studentLastName.length > 0)
	{
	  var params =
      {
        TableName : tableName,
	    Limit : pageLimit,
	    IndexName: "studentLastNameGsi",
	    KeyConditionExpression: "studentLastName = :studentLastName",
	    ExpressionAttributeValues:
	    {
	  	  ":studentLastName": inputObject.studentLastName
	    }
      };
	}
	//If schoolId exists, perform a query on DynamoDB to return limited pages until we get all records.
	else if(inputObject.hasOwnProperty("schoolId") && inputObject.schoolId !== undefined && inputObject.schoolId !== "" && inputObject.schoolId.length > 0)
	{
	  var params =
      {
        TableName : tableName,
	    Limit : pageLimit,
	    KeyConditionExpression: "schoolId = :schoolId",
	    ExpressionAttributeValues:
	    {
	      ":schoolId": inputObject.schoolId
	    }
      };
	}
	else
	{
	  var errMsg = "Query not supported! Your query object should supply: (schoolId, studentId) OR (studentLastName) OR (schoolId)";
	  console.log(errMsg);
	  throw new Error(errMsg);
	}
  }

  return params;
}

exports.getQueryParams = getQueryParams;

exports.handler = async (event) => {
  //Use the AWS.DynamoDB.DocumentClient to write a query against the 'SchoolStudents' table and return the results.
  //The 'SchoolStudents' table key is composed of schoolId (partition key) and studentId (range key).
  console.log("Querying object from DynamoDB...");

  var resultsArray = [];

  try
  {
	var params = getQueryParams(event);
  }
  catch(err)
  {
	throw(err);
  }

  //If query object supplies schoolId and studentId, perform a get on DynamoDB to return one object.
  if(params.hasOwnProperty("Key") && params.Key.hasOwnProperty("schoolId") && params.Key.hasOwnProperty("studentId"))
  {
	await dynamodb.get(params).promise()
    .then((data) =>
	{
	  console.log("Data successfully queried from DB.");
	  resultsArray.push(data.Item);
	})
	.catch((err) =>
	{
      var errMsg = "Error while querying from DB:" + err;
	  console.log(errMsg);
	  throw new Error(errMsg);
	});
  }
  //Otherwise, perform a query on DynamoDB returning pages of limited records until all records are returned.
  else
  {
    await dynamodb.query(params).promise()
    .then(async (data) =>
	{
	  console.log("Data successfully queried from DB.");

	  for(item of data.Items)
	  {
	    resultsArray.push(item);
	  }

	  //Do we still have more pages of data to query?
	  while(data.hasOwnProperty("LastEvaluatedKey"))
	  {
	    params.ExclusiveStartKey = data.LastEvaluatedKey;

	    await dynamodb.query(params).promise()
	         .then((morePagesData) =>
			 {
				console.log("Data successfully queried from DB.");

				for(morePagesItem of morePagesData.Items)
				{
					resultsArray.push(morePagesItem)
				}

				data = morePagesData; //Perform this assignment so while loop works as expected.
			 })
			 .catch((err) =>
			 {
			   var errMsg = "Error while querying from DB:" + err;
	            console.log(errMsg);
	           throw new Error(errMsg);
			 });
	  }
	})
	.catch((err) =>
	{
	  var errMsg = "Error while querying from DB:" + err;
	  console.log(errMsg);
	  throw new Error(errMsg);
	});
  }

  return resultsArray;
};