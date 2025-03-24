from sqlalchemy import Column, Integer, String, Date, Numeric, ForeignKey, DateTime, func, BigInteger
from sqlalchemy.orm import relationship
from models.database import Base

class Company(Base):
    __tablename__ = "companies"
    __table_args__ = {'extend_existing': True}  

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255))  
    industry = Column(String(255))
    exchange = Column(String(50))
    ticker = Column(String(50), unique=True, nullable=False)  # Giữ ticker làm định danh duy nhất
    established_year = Column(Integer)
    foreign_percent = Column(Numeric)

    prices = relationship("Price", back_populates="company", cascade="all, delete-orphan")
    financial_ratios = relationship("FinancialRatioQuarter", back_populates="company", cascade="all, delete-orphan")

class Price(Base):
    __tablename__ = "stock_prices"
    __table_args__ = {'extend_existing': True}  

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(50), ForeignKey("companies.ticker"), nullable=False)  # Liên kết với Company
    date = Column(Date, nullable=False)
    open_price = Column(Numeric)
    close_price = Column(Numeric)
    high_price = Column(Numeric)
    low_price = Column(Numeric)
    volume = Column(BigInteger)

    company = relationship("Company", back_populates="prices")

class FinancialRatioQuarter(Base):
    __tablename__ = "financial_ratios_quarter"
    __table_args__ = {'extend_existing': True}  

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(10), ForeignKey("companies.ticker"), nullable=False) 
    quarter = Column(Integer, nullable=False)  
    year = Column(Integer, nullable=False) 
    price_to_earning = Column(Numeric)
    price_to_book = Column(Numeric)
    roa = Column(Numeric)
    debt_on_equity = Column(Numeric)
    ebit_on_interest = Column(Numeric)
    eps_change = Column(Numeric)
    earning_per_share = Column(Numeric)
    current_payment = Column(Numeric)
    quick_payment = Column(Numeric)

    company = relationship("Company", back_populates="financial_ratios_quarter")
    
# class FinancialRatioYear(Base):
#     __tablename__ = "financial_ratios_year"
#     __table_args__ = {'extend_existing': True}  



#     id = Column(Integer, primary_key=True, autoincrement=True)
#     ticker = Column(String(10), ForeignKey("companies.ticker"), nullable=False) 
#     year = Column(Integer, nullable=False) 
#     price_to_earning = Column(Numeric)
#     price_to_book = Column(Numeric)
#     roa = Column(Numeric)
#     debt_on_equity = Column(Numeric)
#     ebit_on_interest = Column(Numeric)
#     eps_change = Column(Numeric)
#     earning_per_share = Column(Numeric)
#     current_payment = Column(Numeric)
#     quick_payment = Column(Numeric)

#     company = relationship("Company", back_populates="financial_ratios_year")
