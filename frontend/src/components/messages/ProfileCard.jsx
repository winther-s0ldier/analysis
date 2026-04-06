import React from 'react';

const DATASET_TYPE_LABEL = {
  event_log:                'Event Log',
  transactional:            'Transactional',
  time_series:              'Time Series',
  funnel:                   'Funnel',
  survey_or_cross_sectional:'Survey',
  tabular_generic:          'General Table',
};

function StatChip({ label, value }) {
  if (value == null) return null;
  return (
    <div className="flex flex-col gap-0.5 px-3 py-2 rounded-lg border" style={{ background: '#F9FAFB', borderColor: '#E5E7EB' }}>
      <span className="text-[10px] font-semibold uppercase tracking-widest" style={{ color: '#9CA3AF' }}>{label}</span>
      <span className="text-[15px] font-bold font-mono" style={{ color: '#111827' }}>{value}</span>
    </div>
  );
}

function TypePill({ label, count, color }) {
  if (!count) return null;
  const colorMap = {
    numeric:     { bg: 'rgba(99,102,241,0.08)',  text: '#6366F1', border: 'rgba(99,102,241,0.18)' },
    categorical: { bg: 'rgba(16,185,129,0.08)',  text: '#059669', border: 'rgba(16,185,129,0.18)' },
    datetime:    { bg: 'rgba(245,158,11,0.08)',  text: '#D97706', border: 'rgba(245,158,11,0.18)' },
  };
  const c = colorMap[color];
  if (!c) return null;
  return (
    <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-[11.5px] font-semibold border"
      style={{ background: c.bg, color: c.text, borderColor: c.border }}>
      {label}
      <span className="font-mono font-bold">{count}</span>
    </span>
  );
}

const ROLE_LABEL = { entity_col: 'Entity', time_col: 'Time', event_col: 'Event', outcome_col: 'Outcome', funnel_col: 'Funnel' };
const ROLE_COLOR = {
  entity_col:  { bg: 'rgba(99,102,241,0.07)',  text: '#6366F1', border: 'rgba(99,102,241,0.15)' },
  time_col:    { bg: 'rgba(245,158,11,0.07)',  text: '#D97706', border: 'rgba(245,158,11,0.15)' },
  event_col:   { bg: 'rgba(16,185,129,0.07)',  text: '#059669', border: 'rgba(16,185,129,0.15)' },
  outcome_col: { bg: 'rgba(59,130,246,0.07)',  text: '#2563EB', border: 'rgba(59,130,246,0.15)' },
  funnel_col:  { bg: 'rgba(168,85,247,0.07)',  text: '#7C3AED', border: 'rgba(168,85,247,0.15)' },
};

function formatDate(dateStr) {
  if (!dateStr) return null;
  try {
    return new Date(dateStr).toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
  } catch {
    return String(dateStr).slice(0, 10);
  }
}

export function ProfileCard({ data }) {
  if (!data || !data.profile) {
    // Last-resort: render a minimal card from whatever top-level fields exist
    const fallbackType = data?.dataset_type || data?.classification?.dataset_type || '';
    const fallbackRows = data?.row_count ?? 0;
    const fallbackCols = data?.column_count ?? 0;
    const fallbackFile = data?.filename || '';
    if (fallbackType || fallbackRows || fallbackCols) {
      const typeLabel = DATASET_TYPE_LABEL[fallbackType] || (fallbackType || 'Dataset').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
      return (
        <div className="w-full rounded-xl border overflow-hidden shadow-sm" style={{ background: '#FAFAF9', borderColor: '#E5E1DC' }}>
          <div className="flex items-center gap-2.5 px-4 py-3 border-b" style={{ background: '#F5F2EE', borderColor: '#E5E1DC' }}>
            <span className="text-[13px] font-semibold" style={{ color: '#1C1612' }}>Dataset Profile</span>
            <span className="ml-auto text-[11px] font-semibold px-2 py-0.5 rounded-full" style={{ background: 'rgba(99,102,241,0.1)', color: '#6366F1' }}>{typeLabel}</span>
          </div>
          <div className="flex gap-3 px-4 py-3 flex-wrap">
            {fallbackFile && <StatChip label="File" value={fallbackFile} />}
            {fallbackRows > 0 && <StatChip label="Rows" value={fallbackRows.toLocaleString()} />}
            {fallbackCols > 0 && <StatChip label="Columns" value={fallbackCols} />}
          </div>
        </div>
      );
    }
    return null;
  }
  const { profile, classification } = data;

  const ct = profile.column_types || {};
  const numericCols     = Array.isArray(ct.numeric)     ? ct.numeric     : [];
  const categoricalCols = Array.isArray(ct.categorical) ? ct.categorical : [];
  const datetimeCols    = Array.isArray(ct.datetime)    ? ct.datetime    : [];

  const roles = classification?.column_roles || profile.column_roles || {};
  const activeRoles = Object.entries(roles).filter(([, v]) => v && v !== 'null');

  const correlations = (profile.correlations || []).slice(0, 4);

  // Human-readable dataset type
  const rawType = classification?.dataset_type || 'tabular_generic';
  const typeLabel = DATASET_TYPE_LABEL[rawType] || rawType.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

  // LLM reasoning sentence
  const reasoning = classification?.reasoning;

  // Date span — prefer the column identified as time_col, fall back to first datetime col
  const columns = profile.columns || [];
  const timeColName = roles.time_col;
  let dateSpan = null;
  const timeCol = columns.find(c => c.name === timeColName && c.type_category === 'datetime')
    || columns.find(c => c.type_category === 'datetime' && c.stats?.date_range_days);
  if (timeCol?.stats?.date_range_days) dateSpan = timeCol.stats;

  return (
    <div
      className="w-full flex flex-col mt-1 rounded-2xl border overflow-hidden"
      style={{ background: '#FFFFFF', borderColor: '#E5E7EB', boxShadow: '0 1px 4px rgba(0,0,0,0.05)' }}
    >
      {/* Header */}
      <div className="flex items-center gap-2.5 px-4 pt-4 pb-3 border-b" style={{ borderColor: '#F3F4F6' }}>
        <span
          className="inline-flex items-center px-2.5 py-0.5 rounded-full text-[11px] font-bold border"
          style={{ background: 'rgba(99,102,241,0.08)', color: '#6366F1', borderColor: 'rgba(99,102,241,0.2)' }}
        >
          {typeLabel}
        </span>
        {classification?.confidence != null && classification.confidence > 0 && (
          <span className="text-[11px]" style={{ color: '#9CA3AF' }}>
            {Math.round(classification.confidence * 100)}% confidence
          </span>
        )}
      </div>

      <div className="px-4 pt-2.5 pb-3 flex flex-col gap-2">
        {/* LLM reasoning */}
        {reasoning && (
          <p
            className="text-[12.5px] leading-relaxed italic"
            style={{
              color: '#6B7280',
              display: '-webkit-box',
              WebkitLineClamp: 2,
              WebkitBoxOrient: 'vertical',
              overflow: 'hidden',
            }}
          >
            {reasoning}
          </p>
        )}

        {/* Stat chips */}
        <div className="flex items-center gap-2 flex-wrap">
          <StatChip label="Rows"    value={profile.row_count?.toLocaleString()} />
          <StatChip label="Columns" value={profile.column_count} />
          {profile.memory_mb != null && (
            <StatChip label="Memory" value={`${parseFloat(profile.memory_mb).toFixed(1)} MB`} />
          )}
          {dateSpan?.date_range_days > 0 && (
            <StatChip label="Span" value={`${dateSpan.date_range_days.toLocaleString()} days`} />
          )}
        </div>

        {/* Date range */}
        {dateSpan?.min_date && dateSpan?.max_date && (
          <div className="flex items-center gap-1.5 text-[11.5px] font-mono" style={{ color: '#9CA3AF' }}>
            <span style={{ color: '#374151', fontWeight: 600 }}>{formatDate(dateSpan.min_date)}</span>
            <span>→</span>
            <span style={{ color: '#374151', fontWeight: 600 }}>{formatDate(dateSpan.max_date)}</span>
          </div>
        )}

        {/* Column type pills */}
        {(numericCols.length + categoricalCols.length + datetimeCols.length) > 0 && (
          <div className="flex items-center gap-1.5 flex-wrap">
            <TypePill label="Numeric"     count={numericCols.length}     color="numeric"     />
            <TypePill label="Categorical" count={categoricalCols.length} color="categorical" />
            <TypePill label="DateTime"    count={datetimeCols.length}    color="datetime"    />
          </div>
        )}

        {/* Column roles */}
        {activeRoles.length > 0 && (
          <div>
            <div className="text-[10px] font-bold uppercase tracking-widest mb-1.5" style={{ color: '#9CA3AF' }}>
              Column Roles
            </div>
            <div className="flex flex-wrap gap-1.5">
              {activeRoles.map(([key, val]) => {
                const c = ROLE_COLOR[key] || { bg: '#F3F4F6', text: '#374151', border: '#E5E7EB' };
                return (
                  <span
                    key={key}
                    className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-[11.5px] font-medium border"
                    style={{ background: c.bg, color: c.text, borderColor: c.border }}
                  >
                    <span className="opacity-60 text-[10px] font-semibold uppercase">{ROLE_LABEL[key] || key.replace(/_col$/, '')}</span>
                    <span className="font-mono font-semibold">{String(val)}</span>
                  </span>
                );
              })}
            </div>
          </div>
        )}

        {/* Top correlations */}
        {correlations.length > 0 && (
          <div>
            <div className="text-[10px] font-bold uppercase tracking-widest mb-1.5" style={{ color: '#9CA3AF' }}>
              Top Correlations
            </div>
            <div className="grid grid-cols-2 gap-1.5">
              {correlations.map((c, i) => {
                const isPositive = c.correlation > 0;
                return (
                  <div
                    key={i}
                    className="flex items-center justify-between gap-2 px-2.5 py-1.5 rounded-lg text-[11.5px] border"
                    style={{ background: '#F9FAFB', borderColor: '#E5E7EB' }}
                  >
                    <span className="font-mono truncate" style={{ color: '#374151' }}>
                      {c.columns?.[0]} × {c.columns?.[1]}
                    </span>
                    <span className="font-bold font-mono shrink-0" style={{ color: isPositive ? '#059669' : '#DC2626' }}>
                      {c.correlation > 0 ? '+' : ''}{c.correlation.toFixed(2)}
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
