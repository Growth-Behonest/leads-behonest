import React from 'react';

function Header() {
  return (
    <header className="header">
      <div className="header-logo">
        <h1>Be <span>Honest</span></h1>
      </div>
      <nav className="header-nav">
        <a href="#dashboard">Dashboard</a>
        <a href="#leads">Leads</a>
        <a href="#relatorios">Relatórios</a>
      </nav>
    </header>
  );
}

export default Header;
