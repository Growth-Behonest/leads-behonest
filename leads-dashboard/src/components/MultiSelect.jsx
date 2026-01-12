import React, { useState, useEffect, useRef } from 'react';

function MultiSelect({ label, options, selected, onChange }) {
    const [isOpen, setIsOpen] = useState(false);
    const containerRef = useRef(null);

    // Fecha o dropdown ao clicar fora
    useEffect(() => {
        function handleClickOutside(event) {
            if (containerRef.current && !containerRef.current.contains(event.target)) {
                setIsOpen(false);
            }
        }
        document.addEventListener("mousedown", handleClickOutside);
        return () => {
            document.removeEventListener("mousedown", handleClickOutside);
        };
    }, []);

    const toggleOption = (option) => {
        const newSelected = selected.includes(option)
            ? selected.filter(item => item !== option)
            : [...selected, option];
        onChange(newSelected);
    };

    const getButtonText = () => {
        if (selected.length === 0) return 'Todas';
        if (selected.length === 1) return selected[0];
        if (selected.length === options.length) return 'Todas';
        return `${selected.length} selecionados`;
    };

    const handleSelectAll = (e) => {
        e.preventDefault(); // Evita fechar ao clicar
        if (selected.length === options.length) {
            onChange([]);
        } else {
            onChange([...options]);
        }
    };

    return (
        <div className="filter-group" ref={containerRef}>
            <label>{label}</label>
            <div className="multiselect-container">
                <button
                    className={`multiselect-button ${isOpen ? 'active' : ''}`}
                    onClick={() => setIsOpen(!isOpen)}
                    type="button"
                >
                    <span>{getButtonText()}</span>
                    <span className="arrow">▼</span>
                </button>

                {isOpen && (
                    <div className="multiselect-dropdown">
                        <div className="multiselect-actions">
                            <button onClick={handleSelectAll} className="btn-text">
                                {selected.length === options.length ? 'Desmarcar Todas' : 'Marcar Todas'}
                            </button>
                        </div>
                        <div className="multiselect-options">
                            {options.map((option, idx) => (
                                <label key={idx} className="multiselect-option">
                                    <input
                                        type="checkbox"
                                        checked={selected.includes(option)}
                                        onChange={() => toggleOption(option)}
                                    />
                                    <span>{option}</span>
                                </label>
                            ))}
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}

export default MultiSelect;
