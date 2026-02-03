# Databricks notebook source
# MAGIC %sql
# MAGIC INSERT INTO projecto1.de_associate.pipeline_runs_log (run_date,run_message,run_status)
# MAGIC SELECT current_timestamp(), 'SUCCESS' ,1;