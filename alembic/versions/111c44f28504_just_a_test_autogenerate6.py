"""just a test autogenerate6

Revision ID: 111c44f28504
Revises: f86cace7a0cd
Create Date: 2018-05-12 02:09:59.879180

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '111c44f28504'
down_revision = 'f86cace7a0cd'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('MarketData_CASH_EUR_CHF_IDEALPRO', sa.Column('diffToNextRowInMinutes', sa.Integer(), nullable=True))
    op.add_column('MarketData_CASH_EUR_CNH_IDEALPRO', sa.Column('diffToNextRowInMinutes', sa.Integer(), nullable=True))
    op.add_column('MarketData_CASH_EUR_GBP_IDEALPRO', sa.Column('diffToNextRowInMinutes', sa.Integer(), nullable=True))
    op.add_column('MarketData_CASH_EUR_JPY_IDEALPRO', sa.Column('diffToNextRowInMinutes', sa.Integer(), nullable=True))
    op.add_column('MarketData_CASH_EUR_RUB_IDEALPRO', sa.Column('diffToNextRowInMinutes', sa.Integer(), nullable=True))
    op.add_column('MarketData_CASH_EUR_USD_IDEALPRO', sa.Column('diffToNextRowInMinutes', sa.Integer(), nullable=True))
    op.add_column('MarketData_CFD_IBDE30_EUR_SMART', sa.Column('diffToNextRowInMinutes', sa.Integer(), nullable=True))
    op.add_column('MarketData_IND_DAX_EUR_DTB', sa.Column('diffToNextRowInMinutes', sa.Integer(), nullable=True))
    op.add_column('MarketData_IND_HSC50_HKD_HKFE', sa.Column('diffToNextRowInMinutes', sa.Integer(), nullable=True))
    op.add_column('MarketData_IND_INDU_USD_CME', sa.Column('diffToNextRowInMinutes', sa.Integer(), nullable=True))
    op.add_column('MarketData_IND_N225_JPY_OSE.JPN', sa.Column('diffToNextRowInMinutes', sa.Integer(), nullable=True))
    op.add_column('MarketData_IND_SPX_USD_CBOE', sa.Column('diffToNextRowInMinutes', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('MarketData_IND_SPX_USD_CBOE', 'diffToNextRowInMinutes')
    op.drop_column('MarketData_IND_N225_JPY_OSE.JPN', 'diffToNextRowInMinutes')
    op.drop_column('MarketData_IND_INDU_USD_CME', 'diffToNextRowInMinutes')
    op.drop_column('MarketData_IND_HSC50_HKD_HKFE', 'diffToNextRowInMinutes')
    op.drop_column('MarketData_IND_DAX_EUR_DTB', 'diffToNextRowInMinutes')
    op.drop_column('MarketData_CFD_IBDE30_EUR_SMART', 'diffToNextRowInMinutes')
    op.drop_column('MarketData_CASH_EUR_USD_IDEALPRO', 'diffToNextRowInMinutes')
    op.drop_column('MarketData_CASH_EUR_RUB_IDEALPRO', 'diffToNextRowInMinutes')
    op.drop_column('MarketData_CASH_EUR_JPY_IDEALPRO', 'diffToNextRowInMinutes')
    op.drop_column('MarketData_CASH_EUR_GBP_IDEALPRO', 'diffToNextRowInMinutes')
    op.drop_column('MarketData_CASH_EUR_CNH_IDEALPRO', 'diffToNextRowInMinutes')
    op.drop_column('MarketData_CASH_EUR_CHF_IDEALPRO', 'diffToNextRowInMinutes')
    # ### end Alembic commands ###
