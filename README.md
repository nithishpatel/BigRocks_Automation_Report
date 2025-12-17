# BigRocks_Automation_Report

The Big Rocks project is a Python‑driven KPI engine that turns complex, multi‑channel operations data into a single, trusted report for senior leadership.

**Big Rocks automation overview**

Built a modular Python codebase with dedicated scripts for calls, Customer Care, Customer Service, Back Office, DIY/self‑service and maintenance, all orchestrated by a central controller script.

Operational data is refreshed on a schedule using Airflow, and the Python engine is run on top of these updated tables to calculate the latest KPIs whenever leadership needs them.

The pipeline generates an executive‑ready Big‑Rocks Excel workbook that the Customer Operations Director, Customer Marketing Director and all departmental heads (Back Office, Customer Care, Customer Service, DIY, Field/Guard teams) use in their daily and weekly reviews.

**KPIs and domain knowledge**

Encodes advanced contact‑centre and security metrics, including:

SLA% for calls and detailed call response times.

WhatsApp response times and messaging service levels.

First Contact Resolution %, desk retention rates, ticket volumes and backlog.

Finalized maintenances and ageing for back‑office/technical cases.

Guard and police callouts, linked back to calls and maintenance activity.

All KPIs are calculated in Python using consistent business rules, providing a single source of truth for performance against the organisation’s “Big Rocks” (service quality, efficiency and customer protection).

**Business impact**

Eliminates manual spreadsheet work by automating KPI calculation and report assembly, so leaders receive an up‑to‑date Big Rocks pack instead of building it themselves.

Aligns every department on the same definitions of SLA, FCR, retention, maintenance completion and callouts, improving decision‑making and accountability.

Demonstrates the ability to own an entire analytics automation stack: from orchestrated data refresh, through Python transformation and KPI logic, to a polished, director‑level reporting asset.
