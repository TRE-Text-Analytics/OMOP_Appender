export const InfoMsg = ({ children }) => {
  return (
    <div className="info-banner" style={{ marginTop: 'var(--space-4)' }}>
      <span style={{ fontWeight: 'bold', fontFamily: 'var(--mono)' }}>ⓘ TIP: </span>
      <span>
        {children}
      </span>
    </div>
  );
};