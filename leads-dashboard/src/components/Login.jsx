import { useState } from 'react';
import './Login.css';

const REQUIRED_PASS = "leadsbehonestsults548@";

function Login({ onLogin }) {
    const [password, setPassword] = useState('');
    const [error, setError] = useState(false);

    const handleSubmit = (e) => {
        e.preventDefault();
        if (password === REQUIRED_PASS) {
            onLogin();
        } else {
            setError(true);
            setPassword('');
        }
    };

    return (
        <div className="login-container">
            <div className="login-card">
                <h2>Acesso Restrito</h2>
                <p>Por favor, insira a senha para acessar o dashboard.</p>

                <form onSubmit={handleSubmit}>
                    <input
                        type="password"
                        placeholder="Senha de acesso"
                        value={password}
                        onChange={(e) => {
                            setPassword(e.target.value);
                            setError(false);
                        }}
                        autoComplete="current-password"
                        autoFocus
                    />
                    {error && <div className="error-msg">Senha incorreta</div>}

                    <button type="submit">Entrar</button>
                </form>
            </div>
        </div>
    );
}

export default Login;
