import logging
import openai
from typing import List, Dict, Optional
from datetime import datetime
import config

logger = logging.getLogger(__name__)

class InsightsGenerator:
    def __init__(self):
        if config.openai_api_key:
            self.enabled = True
        else:
            self.enabled = False
            logger.warning("OpenAI API key not configured, insights disabled")

    async def generate_trade_insights(self, strategy_name: str, trades: List[Dict], currency: str, size: float, premium: float, index_price: float) -> Optional[str]:
        """
        Generate insights for options trading strategy using OpenAI API
        
        Args:
            strategy_name: Name of the trading strategy (e.g., "LONG BTC CALL SPREAD")
            trades: List of trade objects with details
            currency: Currency (BTC/ETH)
            size: Position size
            premium: Net premium paid/received
            index_price: Current underlying price
            
        Returns:
            Insights string (max 100 words) or None if disabled/failed
        """
        if not self.enabled:
            return None
            
        try:
            # Build context for the AI
            context = self._build_trade_context(strategy_name, trades, currency, size, premium, index_price)
            
            # Create prompt for insights
            current_date = datetime.now().strftime("%Y-%m-%d")
            prompt = f"""You are an options trading expert analyzing a block trade from the perspective of the trader who initiated it. Today's date is {current_date}. Based on the following trade information, generate a concise (≤100 words) market insight for Telegram users.

{context}

Analysis Framework:
1. Market Directional Bias: What market view does this trade express? (bullish/bearish/neutral with timeframe)
2. Risk Profile: Describe the trader's risk exposure and positioning intent
3. Key Levels: Highlight important strikes and expiries relevant to this position
4. Volatility Positioning: Is the trader long/short volatility? What's the IV context?
5. Market Signal: What does this positioning suggest about market sentiment?

Requirements:
- Analyze from the trader's perspective who initiated this position
- Focus on market view and positioning intent, not calculations
- Use Greeks data for qualitative risk assessment only
- Be specific with timeframes and levels (e.g., "Bullish bias into July expiry")
- Professional tone for fast-paced traders

Avoid:
- Any mathematical calculations or computations
- Counterparty analysis or market maker perspectives
- Generic commentary without trade-specific context
- Vague timeframes ("soon", "in the future")
- Pure trade structure description (assume reader sees the trade)"""

            client = openai.OpenAI(api_key=config.openai_api_key)
            response = client.chat.completions.create(
                model="gpt-4.1",
                messages=[
                    {"role": "system", "content": "You are an expert options trader providing concise market insights."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=150,
                temperature=0.7
            )
            
            insights = response.choices[0].message.content.strip()
            
            # Ensure it's within word limit
            words = insights.split()
            if len(words) > 100:
                insights = ' '.join(words[:100]) + "..."
                
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate insights: {e}")
            return None

    def _build_trade_context(self, strategy_name: str, trades: List[Dict], currency: str, size: float, premium: float, index_price: float) -> str:
        """Build context string for AI prompt"""
        
        context_parts = [
            f"Strategy: {strategy_name}",
            f"Asset: {currency}",
            f"Current Price: ${index_price:,.2f}",
            f"Position Size: {size}",
            f"Net Premium: {premium:,.4f} {'₿' if currency=='BTC' else 'Ξ'}"
        ]
        
        # Calculate valuable metrics from trades data
        if trades and len(trades) > 0:
            # Calculate aggregated metrics
            total_delta = 0
            total_gamma = 0
            total_vega = 0
            total_theta = 0
            avg_iv = 0
            iv_count = 0
            strikes = []
            expiries = []
            moneyness_values = []
            
            trade_legs = []
            for i, trade in enumerate(trades):
                leg_info = []
                
                # Direction (buy/sell)
                if trade.get("direction"):
                    leg_info.append(trade["direction"].upper())
                
                # Call or Put
                if trade.get("callOrPut"):
                    leg_info.append(trade["callOrPut"].upper())
                
                # Strike and moneyness calculation
                if trade.get("strike"):
                    strike = float(trade["strike"])
                    strikes.append(strike)
                    moneyness = strike / index_price
                    moneyness_values.append(moneyness)
                    leg_info.append(f"${strike}")
                    leg_info.append(f"Moneyness: {moneyness:.2f}")
                
                # Expiry
                if trade.get("expiry"):
                    expiry = trade["expiry"]
                    expiries.append(expiry)
                    leg_info.append(expiry)
                
                # Size
                if trade.get("size"):
                    leg_info.append(f"Size: {trade['size']}")
                
                # IV
                if trade.get("iv"):
                    iv = float(trade["iv"])
                    avg_iv += iv
                    iv_count += 1
                    leg_info.append(f"IV: {iv:.1f}%")
                
                # Greeks and aggregate them
                if trade.get("greeks"):
                    greeks = trade["greeks"]
                    greek_parts = []
                    trade_size = float(trade.get("size", 0))
                    
                    if "delta" in greeks:
                        delta = float(greeks["delta"])
                        total_delta += delta * trade_size
                        greek_parts.append(f"Δ: {delta:.3f}")
                    if "gamma" in greeks:
                        gamma = float(greeks["gamma"])
                        total_gamma += gamma * trade_size
                        greek_parts.append(f"Γ: {gamma:.3f}")
                    if "vega" in greeks:
                        vega = float(greeks["vega"])
                        total_vega += vega * trade_size
                        greek_parts.append(f"ν: {vega:.3f}")
                    if "theta" in greeks:
                        theta = float(greeks["theta"])
                        total_theta += theta * trade_size
                        greek_parts.append(f"Θ: {theta:.3f}")
                    if "rho" in greeks:
                        rho = float(greeks["rho"])
                        greek_parts.append(f"ρ: {rho:.3f}")
                    
                    if greek_parts:
                        leg_info.append(f"Greeks: {', '.join(greek_parts)}")
                
                if leg_info:
                    trade_legs.append(f"Leg {i+1}: {' '.join(leg_info)}")
            
            # Add calculated metrics to context
            context_parts.append("\n--- Calculated Metrics ---")
            
            # Portfolio Greeks
            if total_delta != 0:
                context_parts.append(f"Total Delta Exposure: {total_delta:.2f}")
            if total_gamma != 0:
                context_parts.append(f"Total Gamma Exposure: {total_gamma:.4f}")
            if total_vega != 0:
                context_parts.append(f"Total Vega Exposure: {total_vega:.2f}")
            if total_theta != 0:
                context_parts.append(f"Total Theta Exposure: {total_theta:.2f}")
            
            # IV Analysis
            if iv_count > 0:
                avg_iv = avg_iv / iv_count
                context_parts.append(f"Average IV: {avg_iv:.1f}%")
            
            # Strike Analysis
            if strikes:
                min_strike = min(strikes)
                max_strike = max(strikes)
                context_parts.append(f"Strike Range: ${min_strike} - ${max_strike}")
                
                # Moneyness analysis
                if moneyness_values:
                    avg_moneyness = sum(moneyness_values) / len(moneyness_values)
                    context_parts.append(f"Average Moneyness: {avg_moneyness:.2f}")
                    
                    # Categorize position
                    if avg_moneyness < 0.95:
                        context_parts.append("Position: Deep OTM")
                    elif avg_moneyness < 0.98:
                        context_parts.append("Position: OTM")
                    elif avg_moneyness < 1.02:
                        context_parts.append("Position: ATM")
                    elif avg_moneyness < 1.05:
                        context_parts.append("Position: ITM")
                    else:
                        context_parts.append("Position: Deep ITM")
            
            # Expiry Analysis
            if expiries:
                unique_expiries = list(set(expiries))
                context_parts.append(f"Expiries: {', '.join(unique_expiries)}")
                
                # Calculate DTE (Days to Expiry) for closest expiry
                try:
                    from datetime import datetime
                    closest_expiry = min(unique_expiries)
                    expiry_date = datetime.strptime(closest_expiry, "%d%b%y")
                    current_date = datetime.now()
                    dte = (expiry_date - current_date).days
                    context_parts.append(f"Days to Closest Expiry: {dte}")
                    
                    # Time decay classification
                    if dte <= 7:
                        context_parts.append("Time Decay: High (weekly expiry)")
                    elif dte <= 30:
                        context_parts.append("Time Decay: Medium (monthly expiry)")
                    else:
                        context_parts.append("Time Decay: Low (longer-term)")
                except:
                    pass
            
            # Premium Analysis
            if premium != 0:
                premium_pct = premium * 100
                context_parts.append(f"Premium as % of Spot: {premium_pct:.3f}%")
                
                if premium > 0:
                    context_parts.append("Premium Flow: Net Debit (paid premium)")
                else:
                    context_parts.append("Premium Flow: Net Credit (received premium)")
            
            # Add trade legs
            if trade_legs:
                context_parts.append("\n--- Trade Legs ---")
                context_parts.extend(trade_legs)
        
        return "\n".join(context_parts)

# Global instance
insights_generator = InsightsGenerator()
