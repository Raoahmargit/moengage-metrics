import { NextRequest, NextResponse } from 'next/server';
import https from 'https';
import csvParser from 'csv-parser';

export async function GET(req: NextRequest) {
  const source = req.nextUrl.searchParams.get('source') || 'BOP_PL';

  console.log("🔍 Requested source:", source);

  const csvUrl = process.env[`DATA_URL_${source}`];

  console.log("🌐 Resolved CSV URL:", csvUrl);

  if (!csvUrl) {
    return NextResponse.json({ success: false, error: "Missing data URL for this source" });
  }

  try {
    const csvData: any[] = await fetchCsvFromUrl(csvUrl);

    console.log("📊 Total rows fetched:", csvData.length);

    const loanCompleted = csvData.filter(row => row['MoEngage_User_Event_Loan Completed'] === '1').length;
    const loanRejected = csvData.filter(row => row['MoEngage_User_Event_Loan Rejected'] === '1').length;
    const totalUsers = new Set(csvData.map(row => row['User ID'])).size;

    const metrics = {
      totalUsers,
      loanCompleted,
      loanRejected,
      conversionRate: totalUsers > 0 ? (loanCompleted / totalUsers) * 100 : 0,
    };

    return NextResponse.json({ success: true, metrics });
  } catch (error: any) {
    console.error("❌ Failed to process CSV:", error);
    return NextResponse.json({ success: false, error: error.message });
  }
}

// Utility to fetch and parse CSV from a URL
function fetchCsvFromUrl(url: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    const results: any[] = [];

    https.get(url, response => {
      if (response.statusCode !== 200) {
        return reject(new Error(`Failed to fetch CSV. Status: ${response.statusCode}`));
      }

      response
        .pipe(csvParser())
        .on('data', data => results.push(data))
        .on('end', () => resolve(results))
        .on('error', reject);
    }).on('error', reject);
  });
}
