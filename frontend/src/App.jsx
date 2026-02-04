import React, { useState, useEffect } from 'react';
import './App.css';

export default function App() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [feed, setFeed] = useState([]);
  const [uploading, setUploading] = useState(false);

  useEffect(() => {
    // Initial fetch
    fetch('/api/resized')
      .then(res => res.json())
      .then(data => setFeed(data.images || []));
    // Poll every 3 seconds
    const interval = setInterval(() => {
      fetch('/api/resized')
        .then(res => res.json())
        .then(data => setFeed(data.images || []));
    }, 3000);
    return () => clearInterval(interval);
  }, []);

  const handleFileChange = (e) => {
    setSelectedFile(e.target.files[0]);
  };

  const handleUpload = async (e) => {
    e.preventDefault();
    if (!selectedFile) return;
    setUploading(true);
    const formData = new FormData();
    formData.append('image', selectedFile);
    await fetch('/api/upload', {
      method: 'POST',
      body: formData,
    });
    setUploading(false);
    setSelectedFile(null);
    // Refresh feed
    fetch('/api/resized')
      .then(res => res.json())
      .then(data => setFeed(data.images || []));
  };

  return (
    <div className="container">
      <h1>Image Uploader & Feed</h1>
      <form onSubmit={handleUpload} className="upload-form">
        <input type="file" accept="image/*" onChange={handleFileChange} />
        <button type="submit" disabled={uploading || !selectedFile}>
          {uploading ? 'Uploading...' : 'Upload'}
        </button>
      </form>
      <div className="feed">
        {feed.length === 0 && <p>No images yet.</p>}
        {feed.map((img, idx) => (
          <div key={idx} className="feed-item">
            <img src={`/api/resized/${img}`} alt="Resized" />
          </div>
        ))}
      </div>
    </div>
  );
}
