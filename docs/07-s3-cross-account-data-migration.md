# Cross Account S3 Migration and IAM Replication Procedure

## 1. Overview

This document describes a fully CLI driven procedure to:

* Replicate an IAM user and its permissions from **Account 1** to **Account 2**
* Migrate data from two S3 buckets in Account 1 to two S3 buckets in Account 2
* Capture the exact sequence of AWS CLI commands used, including common errors and fixes

## 2. Notation and Placeholders

Refer the below as an example scenario:

* `ACCOUNT_1_ID` → Source AWS account ID
* `ACCOUNT_2_ID` → Target AWS account ID
* `ACCOUNT_1_USER_ARN` → `arn:aws:iam::ACCOUNT_1_ID:user/aws-pipeline-user`
* `ACCOUNT_2_USER_ARN` → `arn:aws:iam::ACCOUNT_2_ID:user/aws-pipeline-user`
* `SOURCE_LOOKUP_BUCKET` → `noaa-ais-lookup-data`
* `SOURCE_STAGING_BUCKET` → `noaa-ais-staging-data`
* `DEST_LOOKUP_BUCKET` → `ais-lookup`
* `DEST_STAGING_BUCKET` → `ais-staging`

All commands below are meant to run in **CloudShell** for each account, with the correct account selected.

---

## 3. Inspect IAM in Account 1

### 3.1 Identify current caller

```bash
aws sts get-caller-identity
```

Return fields of interest:

* `Account` → `ACCOUNT_1_ID`
* `Arn` → `ACCOUNT_1_USER_ARN`

### 3.2 List IAM users

```bash
aws iam list-users
```

Relevant user for migration:

* `UserName`: `aws-pipeline-user`

### 3.3 List attached AWS managed policies for the user

```bash
aws iam list-attached-user-policies --user-name aws-pipeline-user
```

Example managed policies attached:

* `arn:aws:iam::aws:policy/service-role/AmazonDMSCloudWatchLogsRole`
* `arn:aws:iam::aws:policy/CloudWatchLogsFullAccess`
* `arn:aws:iam::aws:policy/IAMReadOnlyAccess`
* `arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess`
* `arn:aws:iam::aws:policy/AmazonAthenaFullAccess`
* `arn:aws:iam::aws:policy/IAMUserChangePassword`
* `arn:aws:iam::aws:policy/AmazonS3FullAccess`
* `arn:aws:iam::aws:policy/AWSCloudShellFullAccess`

### 3.4 List inline policies for the user

```bash
aws iam list-user-policies --user-name aws-pipeline-user
```

Example inline policies discovered:

* `custombedrock`
* `customeec2`
* `CustomGlueAccess`
* `rdscustom`

### 3.5 Inspect a specific inline policy (CustomGlueAccess)

```bash
aws iam get-user-policy --user-name aws-pipeline-user --policy-name CustomGlueAccess
```

Example policy document:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole",
        "iam:CreatePolicy",
        "iam:AttachRolePolicy",
        "iam:PassRole"
      ],
      "Resource": "*"
    }
  ]
}
```

---

## 4. Recreate IAM user and permissions in Account 2

All commands in this section are executed in **Account 2 CloudShell**.

### 4.1 Create IAM user in Account 2

```bash
aws iam create-user --user-name aws-pipeline-user
```

### 4.2 Attach the same AWS managed policies in Account 2

```bash
aws iam attach-user-policy --user-name aws-pipeline-user --policy-arn arn:aws:iam::aws:policy/service-role/AmazonDMSCloudWatchLogsRole
aws iam attach-user-policy --user-name aws-pipeline-user --policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess
aws iam attach-user-policy --user-name aws-pipeline-user --policy-arn arn:aws:iam::aws:policy/IAMReadOnlyAccess
aws iam attach-user-policy --user-name aws-pipeline-user --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess
aws iam attach-user-policy --user-name aws-pipeline-user --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess
aws iam attach-user-policy --user-name aws-pipeline-user --policy-arn arn:aws:iam::aws:policy/IAMUserChangePassword
aws iam attach-user-policy --user-name aws-pipeline-user --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam attach-user-policy --user-name aws-pipeline-user --policy-arn arn:aws:iam::aws:policy/AWSCloudShellFullAccess
```

### 4.3 Recreate the `CustomGlueAccess` inline policy in Account 2

1. Create a JSON file with the policy contents:

```bash
cat > CustomGlueAccess.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole",
        "iam:CreatePolicy",
        "iam:AttachRolePolicy",
        "iam:PassRole"
      ],
      "Resource": "*"
    }
  ]
}
EOF
```

2. Attach the inline policy to the user:

```bash
aws iam put-user-policy --user-name aws-pipeline-user --policy-name CustomGlueAccess --policy-document file://CustomGlueAccess.json
```

3. Verify inline policies on the user:

```bash
aws iam list-user-policies --user-name aws-pipeline-user
```

At this point, managed policies and the `CustomGlueAccess` inline policy are replicated. Other inline policies can be migrated with the same pattern.

---

## 5. S3 Migration Design

### 5.1 Buckets involved

Account 1 (source):

* `SOURCE_LOOKUP_BUCKET` → `noaa-ais-lookup-data`
* `SOURCE_STAGING_BUCKET` → `noaa-ais-staging-data`

Account 2 (destination):

* `DEST_LOOKUP_BUCKET` → `ais-lookup`
* `DEST_STAGING_BUCKET` → `ais-staging`

### 5.2 Access model

* Data is **read** from Account 1 buckets using an identity in Account 1.
* Data is **written** into Account 2 buckets using a bucket policy that allows a principal from Account 1.
* The effective flow is `Account 1 principal → Account 1 buckets → Account 2 buckets`.

---

## 6. Create destination buckets in Account 2

Run in **Account 2 CloudShell**:

```bash
aws s3 mb s3://ais-lookup
aws s3 mb s3://ais-staging
```

---

## 7. Grant Account 1 permission to write to Account 2 buckets

Run in **Account 2 CloudShell**.

### 7.1 Bucket policy for `DEST_LOOKUP_BUCKET` (ais-lookup)

```bash
aws s3api put-bucket-policy --bucket ais-lookup --policy '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::ACCOUNT_1_ID:root"},"Action":["s3:PutObject","s3:ListBucket"],"Resource":["arn:aws:s3:::ais-lookup","arn:aws:s3:::ais-lookup/*"]}]}'
```

### 7.2 Bucket policy for `DEST_STAGING_BUCKET` (ais-staging)

```bash
aws s3api put-bucket-policy --bucket ais-staging --policy '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::ACCOUNT_1_ID:root"},"Action":["s3:PutObject","s3:ListBucket"],"Resource":["arn:aws:s3:::ais-staging","arn:aws:s3:::ais-staging/*"]}]}'
```

> Important: A common failure was using the wrong bucket name in the `--bucket` parameter or in the `Resource` ARNs. Both must match the actual bucket name exactly.

Example of a **failing** command (for documentation only):

```bash
# Incorrect: bucket name does not match the Resource ARNs
aws s3api put-bucket-policy --bucket ais-lookup-data --policy '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::ACCOUNT_1_ID:root"},"Action":["s3:PutObject","s3:ListBucket"],"Resource":["arn:aws:s3:::ais-lookup","arn:aws:s3:::ais-lookup/*"]}]}'
# Result: MalformedPolicy: Policy has invalid resource
```

The fix is to keep `--bucket` and `Resource` ARNs aligned on the real bucket name.

---

## 8. Optional: Allow Account 2 to read from Account 1 buckets

This step is only needed if you want to run `aws s3 sync` from Account 2 directly. In the final working flow, sync runs from Account 1, so this is optional.

Run in **Account 1 CloudShell** if needed:

```bash
aws s3api put-bucket-policy --bucket noaa-ais-lookup-data --policy '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"ACCOUNT_2_USER_ARN"},"Action":["s3:ListBucket","s3:GetObject"],"Resource":["arn:aws:s3:::noaa-ais-lookup-data","arn:aws:s3:::noaa-ais-lookup-data/*"]}]}'

aws s3api put-bucket-policy --bucket noaa-ais-staging-data --policy '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"ACCOUNT_2_USER_ARN"},"Action":["s3:ListBucket","s3:GetObject"],"Resource":["arn:aws:s3:::noaa-ais-staging-data","arn:aws:s3:::noaa-ais-staging-data/*"]}]}'
```

Attempting to run sync from Account 2 without read access will produce:

```text
An error occurred (AccessDenied) when calling the ListObjectsV2 operation: Access Denied
```

The resolution is to either grant this access explicitly or perform the sync from Account 1.

---

## 9. Run S3 sync from Account 1 to Account 2

The final working model is to execute sync from **Account 1 CloudShell**. Account 1 can read its own buckets, and the bucket policy in Account 2 allows writes.

Run in **Account 1 CloudShell**:

```bash
aws s3 sync s3://noaa-ais-lookup-data s3://ais-lookup
aws s3 sync s3://noaa-ais-staging-data s3://ais-staging
```

If the bucket write policy in Account 2 is missing or incorrect, you will see `AccessDenied` on the destination bucket. Fix the destination bucket policy as in section 7.

---

## 10. Validate migrated data

You can validate object counts from either account. One simple pattern is:

```bash
aws s3 ls s3://noaa-ais-lookup-data --recursive | wc -l
aws s3 ls s3://ais-lookup --recursive | wc -l

aws s3 ls s3://noaa-ais-staging-data --recursive | wc -l
aws s3 ls s3://ais-staging --recursive | wc -l
```

Counts per pair should match, allowing for minor timing differences if buckets are actively written.

---

## 11. Cleanup cross-account permissions

After the migration is complete and validated, remove temporary cross-account access to tighten security.

Run in **Account 2 CloudShell**:

```bash
aws s3api delete-bucket-policy --bucket ais-lookup
aws s3api delete-bucket-policy --bucket ais-staging
```

If you added any bucket policies on the source side in section 8, you can remove or tighten them similarly:

```bash
aws s3api delete-bucket-policy --bucket noaa-ais-lookup-data
aws s3api delete-bucket-policy --bucket noaa-ais-staging-data
```

At this point:

* IAM user and its key permissions are replicated in Account 2.
* S3 data from lookup and staging buckets is migrated.
* Temporary cross-account access has been removed.
