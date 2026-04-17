import React, { useState } from 'react';

export function Login({ onLogin }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');

  const handleSubmit = (e) => {
    e.preventDefault();
    if (username === 'admin' && password === 'password') {
      sessionStorage.setItem('auth', '1');
      onLogin();
    } else {
      setError('Invalid username or password');
    }
  };

  return (
    <div style={{
      minHeight: '100vh', width: '100vw', display: 'flex', alignItems: 'center', justifyContent: 'center',
      background: '#FFFFFF', fontFamily: 'Inter, sans-serif',
    }}>
      <div style={{
        background: '#fff', borderRadius: 16, padding: '40px 36px',
        width: 360, boxShadow: '0 4px 24px rgba(0,0,0,0.10)',
        border: '1px solid #E2E8F0',
      }}>
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', marginBottom: 28 }}>
          <img src="/adhopsun.jpeg" alt="ADOPSHUN" style={{ width: 56, height: 56, borderRadius: 12, objectFit: 'cover', marginBottom: 14 }} />
          <div style={{ fontSize: 18, fontWeight: 700, color: '#0F172A', letterSpacing: '-0.01em' }}>ADOPSHUN</div>
          <div style={{ fontSize: 13, color: '#64748B', marginTop: 4 }}>Sign in to continue</div>
        </div>

        <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
          <div>
            <label style={{ fontSize: 12, fontWeight: 600, color: '#64748B', display: 'block', marginBottom: 5 }}>Username</label>
            <input
              type="text"
              value={username}
              onChange={e => { setUsername(e.target.value); setError(''); }}
              placeholder="admin"
              style={{
                width: '100%', boxSizing: 'border-box', padding: '9px 12px',
                border: '1px solid #CBD5E1', borderRadius: 8, fontSize: 14,
                fontFamily: 'inherit', outline: 'none', color: '#0F172A',
                background: '#F8FAFC',
              }}
              onFocus={e => e.target.style.borderColor = '#FB7185'}
              onBlur={e => e.target.style.borderColor = '#CBD5E1'}
            />
          </div>
          <div>
            <label style={{ fontSize: 12, fontWeight: 600, color: '#64748B', display: 'block', marginBottom: 5 }}>Password</label>
            <input
              type="password"
              value={password}
              onChange={e => { setPassword(e.target.value); setError(''); }}
              placeholder="••••••••"
              style={{
                width: '100%', boxSizing: 'border-box', padding: '9px 12px',
                border: '1px solid #CBD5E1', borderRadius: 8, fontSize: 14,
                fontFamily: 'inherit', outline: 'none', color: '#0F172A',
                background: '#F8FAFC',
              }}
              onFocus={e => e.target.style.borderColor = '#FB7185'}
              onBlur={e => e.target.style.borderColor = '#CBD5E1'}
            />
          </div>
          {error && <div style={{ fontSize: 12, color: '#EF4444', textAlign: 'center' }}>{error}</div>}
          <button
            type="submit"
            style={{
              marginTop: 4, padding: '10px', background: '#FB7185', color: '#fff',
              border: 'none', borderRadius: 8, fontSize: 14, fontWeight: 600,
              cursor: 'pointer', fontFamily: 'inherit', transition: 'background 0.15s',
            }}
            onMouseEnter={e => e.currentTarget.style.background = '#F43F5E'}
            onMouseLeave={e => e.currentTarget.style.background = '#FB7185'}
          >
            Sign In
          </button>
        </form>
      </div>
    </div>
  );
}
