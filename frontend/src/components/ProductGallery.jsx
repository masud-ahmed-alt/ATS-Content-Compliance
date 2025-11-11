import React from "react";
import ProductCard from "./ProductCard";
import keywordsData from "../assets/enhanced_keywords_sample.json";

export default function ProductGallery({ maxPerCategory = 6 }) {
  const categories = keywordsData.categories || [];

  return (
    <div className="card shadow-sm border-0 mt-4">
      <div className="card-body">
        <div className="d-flex justify-content-between align-items-center mb-3">
          <h5 className="fw-semibold text-primary mb-0">
            Product Gallery (Reference)
          </h5>
          <small className="text-muted">Reference taxonomy for detection</small>
        </div>

        <div className="row g-3">
          {categories.map((cat) => (
            <div key={cat.name} className="col-12">
              <div className="mb-2 d-flex justify-content-between align-items-start">
                <div>
                  <h6 className="mb-0">{cat.name}</h6>
                  <small className="text-muted">{cat.description}</small>
                </div>
                <span className="badge bg-info text-dark">
                  {cat.products.length} products
                </span>
              </div>

              <div className="row row-cols-1 row-cols-sm-2 row-cols-md-3 g-3 mb-3">
                {cat.products.slice(0, maxPerCategory).map((p) => (
                  <div key={p.name} className="col">
                    <ProductCard product={p} />
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
