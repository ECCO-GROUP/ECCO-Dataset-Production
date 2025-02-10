
## Docker build notes:

### Local builds:

From repository top-level:

- Build base image:

        $ docker image build --platform linux/amd64 -t ecco_dataset_production -f containers/docker/Dockerfile.base .
        #                    ^^^^^^^^^^^^^^^^^^^^^^
        #                             |------------ on Apple silicon


- Build "executable" image(s), e.g.:

        $ docker image build --platform linux/amd64 -t edp_generate_dataproducts -f containers/docker/Dockerfile.edp_generate_dataproducts .
        #                    ^^^^^^^^^^^^^^^^^^^^^^
        #                             |------------ on Apple silicon

### AWS ECR destination build:

- Build base image as above.

- Build, and push "executable" image(s) to AWS ECR, e.g:

        # If not already in the environment (not required, just for convenience):
        $ AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
        $ AWS_REGION=$(aws configure get region)

        $ docker image build --platform linux/amd64 -t ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/edp_generate_dataproducts:latest -f containers/docker/Dockerfile.edp_generate_dataproducts .
        #                    ^^^^^^^^^^^^^^^^^^^^^^
        #                             |------------ on Apple silicon

- Create a private repository:

Ref. https://<aws_region>.console.aws.amazon.com/ecr/private-registry/repositories/create?region=<aws_region>

- And push to it:

        $ aws ecr get-login-password | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
        $ docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/edp_generate_dataproducts:latest

### Pulling the image from AWS ECR to an EC2 instance:

- From the EC2 instance:

        # To see if AWS ECR is visible from the EC2 instance (implicit check on IAM policies):
        $ aws ecr describe-repositories

        # use values from push above for <aws_account_id> and <aws_region>:
        $ sudo docker pull <aws_account_id>.dkr.ecr.<aws_region>.amazonaws.com/edp_generate_dataproducts
