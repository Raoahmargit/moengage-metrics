import https from 'https';
import { parse } from 'csv-parse';
import { IncomingMessage } from 'http';

export async function fetchCsvFromUrl(url: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    const results: any[] = [];

    https.get(url, (res: IncomingMessage) => {
      // Handle redirects manually (Google uses 303)
      if (res.statusCode === 303 && res.headers.location) {
        return https.get(res.headers.location, (redirectedRes) => {
          parseCsvStream(redirectedRes, results, resolve, reject);
        });
      }

      // Direct download worked
      if (res.statusCode === 200) {
        parseCsvStream(res, results, resolve, reject);
      } else {
        reject(new Error(`Request failed. Status Code: ${res.statusCode}`));
      }
    }).on('error', reject);
  });
}

function parseCsvStream(
  stream: IncomingMessage,
  results: any[],
  resolve: (value: any[]) => void,
  reject: (reason?: any) => void
) {
  stream
    .pipe(parse({ columns: true }))
    .on('data', (data) => results.push(data))
    .on('end', () => resolve(results))
    .on('error', reject);
}
