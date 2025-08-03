import { google } from 'googleapis';
import { NextRequest, NextResponse } from 'next/server';
import fetch from 'node-fetch';
import { parse } from 'csv-parse';
import { Readable } from 'stream';

// Load credentials from environment variable
const serviceAccount = JSON.parse(
  Buffer.from(process.env.GOOGLE_SERVICE_ACCOUNT as string, 'base64').toString('utf-8')
);

console.log("BOP_PL URL from env:", process.env.DATA_URL_BOP_PL);
console.log("Requested source:", source);

export async function GET(req: NextRequest) {
  const urlParam = req.nextUrl.searchParams.get('source') || 'BOP_PL';
  const dataUrl = process.env[`DATA_URL_${urlParam}`];

  console.log('🔗 Using CSV URL:', dataUrl);

  if (!dataUrl) {
    return NextResponse.json({ success: false, error: 'Missing data URL for this source' });
  }

  try {
    const res = await fetch(dataUrl);
    if (!res.ok) {
      throw new Error(`Request failed. Status Code: ${res.status}`);
    }

    const loans: any[] = [];

    const stream = res.body as Readable;
    await new Promise<void>((resolve, reject) => {
      stream
        .pipe(parse({ columns: true, skip_empty_lines: true }))
        .on('data', (row) => {
          if (
            row['event_name'] === 'loan_disbursed' &&
            row['event_time']?.startsWith('2025-08-03') // <-- replace with dynamic week range later
          ) {
            loans.push(row);
          }
        })
        .on('end', resolve)
        .on('error', reject);
    });

    const loansDisbursedThisWeek = loans.length;

    // Auth with Google Sheets API
    const auth = new google.auth.GoogleAuth({
      credentials: serviceAccount,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    const sheets = google.sheets({ version: 'v4', auth });

    // Append to the sheet
    await sheets.spreadsheets.values.append({
      spreadsheetId: process.env.SHEET_ID,
      range: 'Sheet1!A:B',
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: {
        values: [[new Date().toISOString(), loansDisbursedThisWeek]],
      },
    });

    return NextResponse.json({ success: true, count: loansDisbursedThisWeek });
  } catch (err: any) {
    console.error(err);
    return NextResponse.json({ success: false, error: err.message });
  }
}
