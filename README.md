# Welcome to ajust maintenance window

npm install     // Install the package
cdk bootstrap   // The first time you deploy an AWS CDK app into an environment (account/region), you can install a “bootstrap stack” 
cdk deploy      // Deploy the project

input example for step functions
{
  "region": "us-west-2",
  "clusterIds": ["aurora-gdb-uswest2-test2", "aurora-gdb-uswest2-test3"]
}