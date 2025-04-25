
## Docker build notes:

### Local builds:

From repository top-level:

- Build base image:

        $ docker image build --platform linux/amd64 -t ecco_dataset_production -f ./containers/docker/Dockerfile.base .
        #                    ^^^^^^^^^^^^^^^^^^^^^^
        #                             |------------ on Apple silicon


- Build "executable" image(s), e.g.:

        $ docker image build --platform linux/amd64 -t ecco_edp_generate_dataproducts -f ./containers/docker/Dockerfile.edp_generate_dataproducts .
        #                    ^^^^^^^^^^^^^^^^^^^^^^
        #                             |------------ on Apple silicon

Note that although image tags can be anything, AWS IAM policies that have been
implemented at JPL for the ECCO project require AWS ECR repository image tags to begin
with the string 'ecco'.

### AWS ECR destination build:

- Build base image as above.

- Build, and push "executable" image(s) to AWS ECR, e.g:

        # If not already in the environment (not required, just for convenience):
        $ AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
        $ AWS_REGION=$(aws configure get region)

        $ docker image build --platform linux/amd64 -t ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/ecco_edp_generate_dataproducts:latest -f ./containers/docker/Dockerfile.edp_generate_dataproducts .
        #                    ^^^^^^^^^^^^^^^^^^^^^^
        #                             |------------ on Apple silicon

- Create a private repository:

Ref. https://<aws_region>.console.aws.amazon.com/ecr/private-registry/repositories/create?region=<aws_region>

- And push to it:

        $ aws ecr get-login-password | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
        $ docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/ecco_edp_generate_dataproducts:latest

### Pulling the image from AWS ECR to an EC2 instance:

- From the EC2 instance:

        # To see if AWS ECR is visible from the EC2 instance (implicit check on IAM policies):
        $ aws ecr describe-repositories

        # for ease-of-use, set AWS_ACCOUNT_ID and AWS_REGION as above

        # authenticate, as above using `aws ecr get-login-password ...`. Note
        # that, depending on the installation, all docker commands may need to
        # be run as superuser (root). Should that be the case, authenticate
        # using (note `sudo docker`):

        $ aws ecr get-login-password | sudo docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

        # and pull from repo:
        $ sudo docker pull ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/ecco_edp_generate_dataproducts

        # and run using, e.g.:
        $ sudo docker run --rm --env TASKLIST=s3://<bucket_and_prefix>/some_task_list.json ecco_edp_generate_dataproducts
        # where TASKLIST must reference an S3-resident task list


