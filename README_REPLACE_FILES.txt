REPLACEMENT BUNDLE - ALAN OPERATOR SITE

Files in this bundle:
- index.html
- client-intake.html
- site-config.js
- sample.pdf (unchanged, included for completeness)
- sample-preview.png
- renders/page-1.png
- renders/page-2.png
- favicon.png
- apple-touch-icon.png

WHAT CHANGED
1. Removed the public "Client intake" nav link from the homepage.
2. Clarified the paid flow everywhere:
   Buy -> get intake access after checkout -> submit notes -> get packet back.
3. Made the intake page support a direct paid-client link:
   client-intake.html?code=YOUR_CODE
   It auto-redeems the code against the backend and opens the real Tally form.
4. Kept the sample PDF unchanged because it already shows the product well and changing it would add risk without clear conversion upside.

REQUIRED BACKEND / RAILWAY VARS
Set these on Railway:
- CLIENT_GATE_ALLOWED_ORIGINS=https://nalalalan.github.io
- CLIENT_INTAKE_URL=https://tally.so/r/2EJlrg
- CLIENT_GATE_CODES=northline-live-20260416

BETTER BACKEND VAR (optional)
You can use this instead of CLIENT_GATE_CODES:
CLIENT_GATE_CODES_JSON=[{"code":"northline-live-20260416","label":"paid-client","client_form_url":"https://tally.so/r/2EJlrg"}]

POST-PAYMENT EMAIL / AUTO-REPLY
Send buyers to this exact URL after payment:
https://nalalalan.github.io/alan-operator-site/client-intake.html?code=northline-live-20260416

RECOMMENDED ZERO-TOUCH BUYER REPLY
totally - easiest way to try it is on one real call

buy here: https://buy.stripe.com/bJe28t7Sa7DLgec29A2Nq08

after checkout you'll get the intake link automatically

you'll get a client-ready recap, next steps, and follow-up draft back fast

DEPLOY STEPS
1. Replace the files in your site repo with the files from this bundle.
2. Commit and push:
   git add .
   git commit -m "tighten paid intake flow and remove public intake nav"
   git push
3. Wait for GitHub Pages to update.
