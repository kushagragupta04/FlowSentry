'use client'

import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

// ── Types ─────────────────────────────────────────────────────
interface QueueItem {
  transaction_id: string
  account_id: string
  amount: number
  merchant_id: string
  geo_country: string
  billing_country: string
  shipping_country: string
  risk_score: number
  decision: 'flag' | 'block'
  triggered_rules: string[]
  decision_time: string
  note_text: string | null
  note_ready_at: string | null
  resolution: 'resolved' | 'false_positive' | null
  resolved_at: string | null
  queue_status: 'awaiting_note' | 'pending_review' | 'resolved'
}

interface Stats {
  total_flagged: number
  pending_review: number
  resolved_today: number
  blocked: number
}

// ── Utilities ─────────────────────────────────────────────────
function getRiskColor(score: number): string {
  if (score >= 0.7) return '#f87171'
  if (score >= 0.3) return '#fbbf24'
  return '#34d399'
}

function formatAmount(n: number): string {
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(n)
}

function formatTime(iso: string): string {
  return new Date(iso).toLocaleString('en-US', {
    month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
  })
}

function getRuleLabel(rule: string): string {
  const map: Record<string, string> = {
    country_mismatch_high_value: 'Country Mismatch + High Value',
    rapid_multi_country_velocity: 'Multi-Country Velocity',
    geo_billing_country_mismatch: 'Geo ≠ Billing Country',
  }
  return map[rule] || rule.replace(/_/g, ' ')
}

// ── Detail Modal ───────────────────────────────────────────────
function DetailModal({
  item,
  onClose,
  onResolve,
}: {
  item: QueueItem
  onClose: () => void
  onResolve: (id: string, resolution: 'resolved' | 'false_positive') => void
}) {
  const [resolving, setResolving] = useState(false)

  const handleResolve = async (resolution: 'resolved' | 'false_positive') => {
    setResolving(true)
    await onResolve(item.transaction_id, resolution)
    setResolving(false)
    onClose()
  }

  const alreadyResolved = !!item.resolution

  return (
    <div className="modal-overlay" onClick={(e) => e.target === e.currentTarget && onClose()}>
      <div className="modal-panel" role="dialog" aria-modal="true" aria-label="Transaction detail">
        <div className="modal-header">
          <div>
            <div className="modal-title">Transaction Investigation</div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2, fontFamily: 'JetBrains Mono, monospace' }}>
              {item.transaction_id}
            </div>
          </div>
          <button className="modal-close" onClick={onClose} aria-label="Close">✕</button>
        </div>

        {/* Decision badge + risk score */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 20 }}>
          <span className={`badge ${item.decision}`}>{item.decision}</span>
          <div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>Risk Score</div>
            <div style={{ fontSize: 20, fontWeight: 700, color: getRiskColor(item.risk_score), fontFamily: 'JetBrains Mono, monospace' }}>
              {(item.risk_score * 100).toFixed(1)}%
            </div>
          </div>
          <div style={{ marginLeft: 'auto', textAlign: 'right' }}>
            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>Time</div>
            <div style={{ fontSize: 12, color: 'var(--text-secondary)' }}>{formatTime(item.decision_time)}</div>
          </div>
        </div>

        {/* Transaction details */}
        <div className="detail-section">
          <div className="detail-section-title">Transaction Details</div>
          <div className="detail-grid">
            <div className="detail-item">
              <div className="detail-item-label">Amount</div>
              <div className="detail-item-value" style={{ color: 'var(--accent-blue)' }}>{formatAmount(item.amount)}</div>
            </div>
            <div className="detail-item">
              <div className="detail-item-label">Merchant</div>
              <div className="detail-item-value">{item.merchant_id}</div>
            </div>
            <div className="detail-item">
              <div className="detail-item-label">Billing Country</div>
              <div className="detail-item-value">{item.billing_country}</div>
            </div>
            <div className="detail-item">
              <div className="detail-item-label">Shipping Country</div>
              <div className="detail-item-value" style={{ color: item.billing_country !== item.shipping_country ? '#fbbf24' : 'inherit' }}>
                {item.shipping_country}
                {item.billing_country !== item.shipping_country && ' ⚠'}
              </div>
            </div>
            <div className="detail-item">
              <div className="detail-item-label">Geo Location</div>
              <div className="detail-item-value">{item.geo_country}</div>
            </div>
            <div className="detail-item">
              <div className="detail-item-label">Account</div>
              <div className="detail-item-value" style={{ fontSize: 11 }}>{item.account_id}</div>
            </div>
          </div>
        </div>

        {/* Triggered rules */}
        {item.triggered_rules?.length > 0 && (
          <div className="detail-section">
            <div className="detail-section-title">Triggered Rules</div>
            <div className="rules-list">
              {item.triggered_rules.map(rule => (
                <span key={rule} className="rule-chip">{getRuleLabel(rule)}</span>
              ))}
            </div>
          </div>
        )}

        {/* LLM Investigation Note */}
        <div className="detail-section">
          <div className="detail-section-title" style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span>AI Investigation Note</span>
            <span style={{ fontSize: 10, padding: '2px 6px', background: 'rgba(99,102,241,0.15)', color: '#818cf8', borderRadius: 4, fontFamily: 'JetBrains Mono, monospace' }}>Groq llama3-70b</span>
          </div>
          <div className="note-box">
            {item.note_text
              ? item.note_text
              : <span className="note-loading">⏳ Generating investigation note…</span>}
          </div>
        </div>

        {/* Resolution actions */}
        {!alreadyResolved ? (
          <div className="action-row">
            <button
              id={`resolve-${item.transaction_id}`}
              className="btn btn-resolve"
              onClick={() => handleResolve('resolved')}
              disabled={resolving}
            >
              ✓ Mark Resolved
            </button>
            <button
              id={`fp-${item.transaction_id}`}
              className="btn btn-fp"
              onClick={() => handleResolve('false_positive')}
              disabled={resolving}
            >
              ✗ False Positive
            </button>
          </div>
        ) : (
          <div style={{ marginTop: 16, padding: '10px 14px', background: 'rgba(255,255,255,0.04)', borderRadius: 8, fontSize: 13, color: 'var(--text-secondary)' }}>
            ✓ Resolved as <strong style={{ color: 'var(--text-primary)' }}>{item.resolution}</strong> at {item.resolved_at ? formatTime(item.resolved_at) : '—'}
          </div>
        )}
      </div>
    </div>
  )
}

// ── Main Dashboard Page ────────────────────────────────────────
export default function DashboardPage() {
  const [items, setItems] = useState<QueueItem[]>([])
  const [stats, setStats] = useState<Stats | null>(null)
  const [filter, setFilter] = useState<'all' | 'pending_review' | 'block'>('all')
  const [selected, setSelected] = useState<QueueItem | null>(null)
  const [loading, setLoading] = useState(true)
  const [lastRefresh, setLastRefresh] = useState(new Date())

  const fetchQueue = useCallback(async () => {
    try {
      const [queueRes, statsRes] = await Promise.all([
        axios.get(`${API}/api/queue?status=${filter === 'all' ? '' : filter}&limit=100`),
        axios.get(`${API}/api/stats`),
      ])
      setItems(queueRes.data)
      setStats(statsRes.data)
      setLastRefresh(new Date())
    } catch (e) {
      console.error('Failed to fetch queue:', e)
    } finally {
      setLoading(false)
    }
  }, [filter])

  useEffect(() => {
    fetchQueue()
    const interval = setInterval(fetchQueue, 10_000) // refresh every 10s
    return () => clearInterval(interval)
  }, [fetchQueue])

  const handleResolve = async (txId: string, resolution: 'resolved' | 'false_positive') => {
    try {
      await axios.post(`${API}/api/resolve`, { transaction_id: txId, resolution })
      await fetchQueue()
    } catch (e) {
      console.error('Resolution failed:', e)
    }
  }

  const filteredItems = items.filter(item => {
    if (filter === 'all') return true
    if (filter === 'pending_review') return item.queue_status === 'pending_review'
    if (filter === 'block') return item.decision === 'block'
    return true
  })

  return (
    <div className="app-shell">
      {/* Top bar */}
      <header className="topbar">
        <div className="topbar-brand">
          <div className="shield-icon">🛡️</div>
          <h1>FraudGuard</h1>
          <span style={{ fontSize: 11, color: 'var(--text-muted)', paddingLeft: 4 }}>Analyst Dashboard</span>
        </div>
        <div className="topbar-status">
          <div className="status-dot" />
          <span>Live · refreshed {lastRefresh.toLocaleTimeString()}</span>
        </div>
      </header>

      <main className="content">
        {/* Stats */}
        <div className="stats-row">
          <div className="stat-card total">
            <div className="stat-label">Flagged (all time)</div>
            <div className="stat-value">{stats?.total_flagged ?? '—'}</div>
            <div className="stat-sub">Requires investigation</div>
          </div>
          <div className="stat-card flag">
            <div className="stat-label">Pending Review</div>
            <div className="stat-value">{stats?.pending_review ?? '—'}</div>
            <div className="stat-sub">Awaiting analyst action</div>
          </div>
          <div className="stat-card block">
            <div className="stat-label">Blocked</div>
            <div className="stat-value">{stats?.blocked ?? '—'}</div>
            <div className="stat-sub">Hard-blocked transactions</div>
          </div>
          <div className="stat-card resolved">
            <div className="stat-label">Resolved Today</div>
            <div className="stat-value">{stats?.resolved_today ?? '—'}</div>
            <div className="stat-sub">Confirmed or marked FP</div>
          </div>
        </div>

        {/* Queue */}
        <div className="queue-header">
          <div className="queue-title">Investigation Queue</div>
          <div className="queue-filter">
            {(['all', 'pending_review', 'block'] as const).map(f => (
              <button
                key={f}
                id={`filter-${f}`}
                className={`filter-btn${filter === f ? ' active' : ''}`}
                onClick={() => setFilter(f)}
              >
                {f === 'all' ? 'All' : f === 'pending_review' ? 'Pending' : 'Blocked'}
              </button>
            ))}
          </div>
        </div>

        <div className="queue-table-wrap">
          {loading ? (
            <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-muted)' }}>
              Loading queue…
            </div>
          ) : filteredItems.length === 0 ? (
            <div className="empty-state">
              <div className="icon">✅</div>
              <p>No items in queue matching current filter</p>
            </div>
          ) : (
            <table className="queue-table" role="grid">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Decision</th>
                  <th>Account</th>
                  <th>Amount</th>
                  <th>Countries</th>
                  <th>Risk Score</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {filteredItems.map(item => (
                  <tr
                    key={item.transaction_id}
                    className="clickable"
                    onClick={() => setSelected(item)}
                    role="button"
                    tabIndex={0}
                    onKeyDown={e => e.key === 'Enter' && setSelected(item)}
                    id={`row-${item.transaction_id}`}
                  >
                    <td style={{ color: 'var(--text-secondary)', fontSize: 12 }}>
                      {formatTime(item.decision_time)}
                    </td>
                    <td>
                      <span className={`badge ${item.decision}`}>{item.decision}</span>
                    </td>
                    <td style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 12 }}>
                      {item.account_id}
                    </td>
                    <td style={{ fontWeight: 600, color: 'var(--accent-blue)' }}>
                      {formatAmount(item.amount)}
                    </td>
                    <td style={{ fontSize: 12 }}>
                      <span style={{ color: item.billing_country !== item.shipping_country ? '#fbbf24' : 'var(--text-secondary)' }}>
                        {item.billing_country} → {item.shipping_country}
                      </span>
                    </td>
                    <td>
                      <div className="risk-bar-wrap">
                        <div className="risk-bar-bg">
                          <div
                            className="risk-bar-fill"
                            style={{
                              width: `${item.risk_score * 100}%`,
                              background: getRiskColor(item.risk_score),
                            }}
                          />
                        </div>
                        <div className="risk-val" style={{ color: getRiskColor(item.risk_score) }}>
                          {(item.risk_score * 100).toFixed(1)}%
                        </div>
                      </div>
                    </td>
                    <td>
                      {item.queue_status === 'resolved' ? (
                        <span className="badge allow">{item.resolution === 'false_positive' ? 'False Positive' : 'Resolved'}</span>
                      ) : item.queue_status === 'pending_review' ? (
                        <span className="badge pending">Review</span>
                      ) : (
                        <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Generating note…</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </main>

      {/* Detail modal */}
      {selected && (
        <DetailModal
          item={selected}
          onClose={() => setSelected(null)}
          onResolve={handleResolve}
        />
      )}
    </div>
  )
}
