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
            prompt = f"""You are an options trading expert analyzing a block trade. Today's date is {current_date}. Based on the following trade information, generate a concise (≤100 words) market insight for Telegram users.

{context}

Focus on:

Market direction & timing (bullish/bearish/neutral; short/medium/long-term)

Risk/reward setup (defined risk, convexity, premium flow)

Key strikes & expiries (levels that matter)

Volatility view (long/short vol, IV context)

Positioning/sentiment signal (dealer flow, hedging, crowd behavior)

Tone: professional, actionable, to the point. For fast-paced traders.

✅ Do:

Be specific, e.g. “Bullish bias into July expiry” or “Long vol via OTM put spread”

Emphasize trade intent, structure logic, and possible market implications

❌ Do not:

Do not describe the trade structure only (assume the reader has seen it)

Do not give generic commentary (e.g. “Vol is elevated” without context)

Do not speculate without evidence from trade data

Do not use vague timeframes (e.g. “soon”, “in the future”)"""

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
        
        # Add detailed trade leg information for better LLM understanding
        if trades and len(trades) > 0:
            trade_legs = []
            for i, trade in enumerate(trades):
                leg_info = []
                
                # Direction (buy/sell)
                if trade.get("direction"):
                    leg_info.append(trade["direction"].upper())
                
                # Call or Put
                if trade.get("callOrPut"):
                    leg_info.append(trade["callOrPut"].upper())
                
                # Strike
                if trade.get("strike"):
                    leg_info.append(f"${trade['strike']}")
                
                # Expiry
                if trade.get("expiry"):
                    leg_info.append(trade["expiry"])
                
                # Size
                if trade.get("size"):
                    leg_info.append(f"Size: {trade['size']}")
                
                # IV
                if trade.get("iv"):
                    leg_info.append(f"IV: {trade['iv']:.1f}%")
                
                if leg_info:
                    trade_legs.append(f"Leg {i+1}: {' '.join(leg_info)}")
            
            if trade_legs:
                context_parts.extend(trade_legs)
        
        return "\n".join(context_parts)

# Global instance
insights_generator = InsightsGenerator()
