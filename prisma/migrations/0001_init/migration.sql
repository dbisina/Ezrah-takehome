-- CreateSchema
CREATE SCHEMA IF NOT EXISTS "public";

-- CreateEnum
CREATE TYPE "CredentialType" AS ENUM ('EmploymentCredential', 'IdentityCredential', 'KYCCredential');

-- CreateEnum
CREATE TYPE "PipelineStep" AS ENUM ('IDENTITY_VERIFICATION', 'CLAIMS_VALIDATION', 'CREDENTIAL_SIGNING');

-- CreateEnum
CREATE TYPE "RequestStatus" AS ENUM ('ACCEPTED', 'PROCESSING', 'COMPLETED', 'FAILED');

-- CreateEnum
CREATE TYPE "StepState" AS ENUM ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED');

-- CreateEnum
CREATE TYPE "EventType" AS ENUM ('REQUEST_ACCEPTED', 'STEP_STARTED', 'STEP_SUCCEEDED', 'STEP_FAILED', 'REQUEST_COMPLETED', 'REQUEST_FAILED');

-- CreateEnum
CREATE TYPE "CallbackStatus" AS ENUM ('PENDING', 'DELIVERED', 'FAILED', 'EXHAUSTED');

-- CreateTable
CREATE TABLE "credential_request" (
    "id" UUID NOT NULL,
    "idempotency_key" TEXT NOT NULL,
    "subject_did" TEXT NOT NULL,
    "credential_type" "CredentialType" NOT NULL,
    "claims" JSONB NOT NULL,
    "callback_url" TEXT,
    "status" "RequestStatus" NOT NULL DEFAULT 'ACCEPTED',
    "current_step" "PipelineStep",
    "signed_credential" JSONB,
    "failure_step" "PipelineStep",
    "failure_reason" TEXT,
    "last_sequence" INTEGER NOT NULL DEFAULT 0,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,
    "completed_at" TIMESTAMP(3),

    CONSTRAINT "credential_request_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "step_execution" (
    "id" UUID NOT NULL,
    "request_id" UUID NOT NULL,
    "step" "PipelineStep" NOT NULL,
    "state" "StepState" NOT NULL DEFAULT 'PENDING',
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "error" TEXT,
    "started_at" TIMESTAMP(3),
    "finished_at" TIMESTAMP(3),

    CONSTRAINT "step_execution_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "pipeline_event" (
    "id" UUID NOT NULL,
    "request_id" UUID NOT NULL,
    "sequence" INTEGER NOT NULL,
    "type" "EventType" NOT NULL,
    "step" "PipelineStep",
    "status" TEXT NOT NULL,
    "data" JSONB NOT NULL,
    "occurred_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "pipeline_event_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "outbox_message" (
    "id" UUID NOT NULL,
    "aggregate_id" UUID NOT NULL,
    "topic" TEXT NOT NULL,
    "message_key" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "headers" JSONB,
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "visible_after" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "published_at" TIMESTAMP(3),

    CONSTRAINT "outbox_message_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "callback_delivery" (
    "id" UUID NOT NULL,
    "request_id" UUID NOT NULL,
    "terminal_status" "RequestStatus" NOT NULL,
    "url" TEXT NOT NULL,
    "status" "CallbackStatus" NOT NULL DEFAULT 'PENDING',
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "last_error" TEXT,
    "delivered_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "callback_delivery_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "credential_request_idempotency_key_key" ON "credential_request"("idempotency_key");

-- CreateIndex
CREATE INDEX "credential_request_status_idx" ON "credential_request"("status");

-- CreateIndex
CREATE UNIQUE INDEX "step_execution_request_id_step_key" ON "step_execution"("request_id", "step");

-- CreateIndex
CREATE INDEX "pipeline_event_request_id_sequence_idx" ON "pipeline_event"("request_id", "sequence");

-- CreateIndex
CREATE UNIQUE INDEX "pipeline_event_request_id_sequence_key" ON "pipeline_event"("request_id", "sequence");

-- CreateIndex
CREATE INDEX "outbox_message_published_at_visible_after_created_at_idx" ON "outbox_message"("published_at", "visible_after", "created_at");

-- CreateIndex
CREATE INDEX "callback_delivery_request_id_idx" ON "callback_delivery"("request_id");

-- CreateIndex
CREATE UNIQUE INDEX "callback_delivery_request_id_terminal_status_key" ON "callback_delivery"("request_id", "terminal_status");

-- AddForeignKey
ALTER TABLE "step_execution" ADD CONSTRAINT "step_execution_request_id_fkey" FOREIGN KEY ("request_id") REFERENCES "credential_request"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "pipeline_event" ADD CONSTRAINT "pipeline_event_request_id_fkey" FOREIGN KEY ("request_id") REFERENCES "credential_request"("id") ON DELETE CASCADE ON UPDATE CASCADE;
