import { NextRequest, NextResponse } from 'next/server'

const DB_URL = process.env.DATABASE_URL || 'postgresql://fraudguard:fraudguard_secret@postgres:5432/fraudguard'

// NOTE: In a real Next.js app, you'd use a proper DB client like pg or prisma.
// For this API route, we call the scoring-service backend which has the DB connection.
const SCORING_API = process.env.NEXT_PUBLIC_API_URL || 'http://scoring-service:8000'

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url)
  const status = searchParams.get('status') || ''
  const limit = searchParams.get('limit') || '100'

  try {
    const url = `${SCORING_API}/api/queue?status=${status}&limit=${limit}`
    const res = await fetch(url, { next: { revalidate: 5 } })
    const data = await res.json()
    return NextResponse.json(data)
  } catch (e) {
    return NextResponse.json({ error: 'Failed to fetch queue' }, { status: 503 })
  }
}
