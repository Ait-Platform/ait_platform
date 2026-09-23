# Current rollout instruction ? supersedes earlier disk-retirement plan

Deploy R2-first delivery now. Preserve the persistent disk as fallback/master.
Do not delete, overwrite, convert or otherwise alter disk videos. Do not upload
the prepared video repairs in this release. Identify individual playback issues
from the live application after the initial deployment.

R2 availability failures fall back read-only to static/uploads/reading_videos.
SACE assignment and lesson checks still apply before either source is served.
Browser decoder failures after a successful R2 response remain for live diagnosis.
The known public R2 domain is the code default, overridable by environment.

The notes below record earlier investigation; any no-fallback or disk-removal
instructions below are superseded by this rollout instruction.

---

# R2 migration and SACE access — 2026-09-23

Status: local changes prepared; NOT deployed. Render disk NOT inventoried,
transferred or removed. Live R2 playback NOT verified yet.

## SACE users and subject

Both registration links use auth_subject.slug = sace_endorsement.
Read-only inspection of **ait_local_db** found subject ID **50**,
commercial_mode=free and requires_price=0. IDs must be independently checked
in production. This local database contains no SACE admin or enrolment records.

- Provisioning: Reneilwe / another explicitly appointed SACE administrator.
  Authority comes from auth_subject_admin matching the signed-in email and
  subject slug sace, sace_endorsement or sace_reading. Enrolment alone does
  not grant provisioning authority.
- Join: the Auditor appointed by that administrator. Authority comes from a
  single-use code saved as auditor_provisioned in sace_workshop_interactions,
  claimed_by_user_id and a current Claimed assignment. A new account's
  registration uses sace_endorsement; an already authenticated user can claim
  a code without the claim handler inserting an enrolment.
- sace_teacher (local ID 51) is a separate paid teacher-training track, not
  the endorsement flow.
- Normal SACE sign-in, claim and lesson routes do not require a price.
  Local fixes cover quote entry, registration pricing and dashboard re-entry.
  A returning Auditor is routed to SACE based on their assignment.
  Live user authentication still needs verification against current records.

## Reading video change

The user confirmed bucket **ait-platform-assets**, prefix **reading_videos/**.
Confirmed public domain: https://pub-d9878fa5cbe44074bd45bb83a4376153.r2.dev. Local .env now contains this domain and the reading_videos prefix. Production settings have not been changed.

app/utils/reading_media.py resolves the exact, case-sensitive video filename.
Set R2_PUBLIC_DOMAIN to the configured public HTTPS base URL. The optional
R2_READING_PREFIX defaults to reading_videos. Neither course falls back to disk.

SACE checks the active assignment and lesson prerequisites, verifies remote
metadata, then redirects to the R2 object and records the served event.
Missing objects/configuration return 503 without falsely recording delivery.

The public-domain mechanism matches the existing ordinary Reading delivery.
It does not make a private bucket public. If the bucket is private, replace
this with authenticated S3 access/presigned URLs before deployment. Do not
place private documents in a publicly accessible assets bucket.

Run against the current service environment, without Flask startup:

    python scripts/verify_reading_r2.py --output /tmp/reading-r2-check.json

For a local database manifest, add --dotenv. An explicit --public-domain
can be used without changing .env. The script checks all 18 lesson objects,
MP4 headers and beginning/end byte ranges. Browser playback/seek and the
Auditor completion interaction must also be tested on the deployed service.

## Known code consumers requiring disk investigation

This is a source-code inventory, not a claim about files on the live mount.

| Consumer | Current paths / code | Remaining work |
| --- | --- | --- |
| Reading videos | static/uploads/reading_videos; SACE and Reading routes | Local R2 delivery prepared; verify remote objects and browser playback |
| SACE provider PDFs | static/uploads/sace; admin/security/routes.py; program_sace/endorsement_routes.py | Migrate user-uploaded PDFs plus upload/download/email/controlled-view paths |
| Cultural Fire media | static/uploads/cfi; program_culturalfire routes/helpers | Migrate upload, serve, replace/delete and processing |
| CPTD evidence | static/uploads/cptd | Migrate evidence upload and retrieval |
| HealthCore documents | static/uploads/healthcore | Preserve privacy; update upload and background document processing |
| Mechanic files | static/uploads/mechanic | Migrate logos, letterheads and disk images; distinguish temporary OCR files |
| Debtors logos | static/uploads/debtors | Migrate upload, rendering and PDF consumers |
| UIP controlled files | instance/uip_documents/<organisation> | Preserve tenant/classification checks and immutable versions; private R2 access |
| SPV files | static/uploads/spv exists locally | Confirm active live owners/readers and migrate them |
| Seed uploads/reports | configurable instance/seeds and report paths | Distinguish durable source data from regenerable reports |
| Logs/caches/temp files | instance and module-specific paths | Inventory before deciding retention; R2 is not a filesystem |

Repository static assets (slides, logos, application PDFs) can remain in Git.
PostgreSQL records do not move to R2. Every file actually on the mounted disk
must be accounted for, including files not recognized by current source code.

## Cutover sequence

1. Obtain current Render service link, disk mount path, symlinks and authoritative
   environment configuration. Do not use credentials from old diagnostic scripts.
2. Run scripts/inventory_persistent_disk.py on the actual mount, writing its
   JSON manifest outside the mount. It hashes every regular file and reports
   symlinks, unreadable files and files changing during inspection.
3. Map each manifest file to an R2 key and public/private storage policy.
   Preserve object names referenced by database records.
4. Copy with content hashes; avoid overwriting different objects. Verify every
   destination's bytes, not merely matching filenames or multipart ETags.
5. Migrate ongoing upload/read/replace/delete paths and background jobs for
   each consumer. A backup copy alone does not make the application disk-free.
6. Freeze uploads for the final delta, verify a fresh inventory against R2,
   deploy and test with disk fallback disabled.
7. Verify real admin login, Auditor code claim/resume, all 18 videos with seeking,
   document retrieval and representative new uploads in every affected program.
8. Remove the disk only after all checks pass and the migration is recoverable.

## Remote verification results

All 18 filenames from ait_local_db were checked against the confirmed domain.
12 passed HEAD, MP4 header and beginning/end range tests.
- Missing (404): te.mp4, ti.mp4, to.mp4.
- Mislabeled Matroska contents: 13potato.mp4, 15CVStems.mp4, 16CvstemsPronun.mp4. These return 200 and support byte ranges but are not MP4 containers.
- The user-provided 11DrawPalm.mp4 also returned a valid MP4 header and 206 range response; it is not one of the 18 filenames in the local lesson table. Do not guess a replacement lesson mapping.
- Render service: srv-d47bhsjipnbc73coe6ag. Browser unavailable in this session; no API credentials configured. Live database, disk inventory and deployment remain unverified.

## Local repair package

User instruction: no production changes until authenticated Render and R2 access is available.

Three corrected videos are in artifacts/r2-video-repairs/: 13potato.mp4, 15CVStems.mp4 and 16CvstemsPronun.mp4. They preserve the H.264 video, convert MP3 audio to AAC, and use an MP4 container with faststart. All three passed a full FFmpeg decode. manifest.json records destination keys, SHA-256 hashes, sizes and codec information; original downloads remain alongside them. This folder is ignored by Git. Nothing has been uploaded.

No local te.mp4, ti.mp4 or to.mp4 was found in the searched project media. Other local videos include 11DrawPalm.mp4, lesson5.mp4 and Presentation1.mp4; do not substitute these without validating the production lesson mapping and intended content. The current 404 findings refer to LOCAL database filenames, not a verified production manifest.
